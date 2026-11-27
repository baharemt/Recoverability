using System;
using System.IO;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

// .NET 7. One owner/process, private directories, local atomic rename.
// All readers/writers must coordinate with this service.
public sealed class FileRecoveryService
{
    private readonly string root, backups;
    private readonly SemaphoreSlim gate = new(1, 1);
    private string Active => Path.Combine(root, "events.txt");
    private string Journal => Path.Combine(root, "pending.json");
    private string OldTemp => Path.Combine(root, "archive.tmp");
    private string NewTemp => Path.Combine(root, "remaining.tmp");
    private string Archive(string id) => Path.Combine(root, $"archive-{id}.txt");
    public sealed record Job(string Id, string Original, string Old, string Remaining);
    public sealed record BackupInfo(string Id, string Hash);

    public FileRecoveryService(string directory, string backupDirectory)
    {
        root = Path.GetFullPath(directory);
        backups = Path.GetFullPath(backupDirectory);
        Directory.CreateDirectory(root);
        Directory.CreateDirectory(backups);
    }

    private static FileStream Create(string path) => new(path, FileMode.CreateNew,
        FileAccess.Write, FileShare.None, 65536, FileOptions.Asynchronous);

    private static async Task<string> HashAsync(string path, CancellationToken ct)
    {
        await using var stream = File.OpenRead(path);
        return Convert.ToHexString(await SHA256.HashDataAsync(stream, ct));
    }

    private static async Task VerifyAsync(string path, string expected, CancellationToken ct)
    {
        if (await HashAsync(path, ct) != expected)
            throw new InvalidDataException($"Hash mismatch: {path}");
    }

    private static async Task CopyAsync(string source, string target, CancellationToken ct)
    {
        await using var input = File.OpenRead(source);
        await using var output = Create(target);
        await input.CopyToAsync(output, 65536, ct);
        await output.FlushAsync(ct);
        output.Flush(true);
    }

    private static async Task PublishJsonAsync<T>(string path, T value, CancellationToken ct)
    {
        string temp = path + ".tmp";
        File.Delete(temp); // Only unpublished staging data.
        await using (var output = Create(temp))
        {
            await JsonSerializer.SerializeAsync(output, value, cancellationToken: ct);
            await output.FlushAsync(ct);
            output.Flush(true);
        }
        File.Move(temp, path); // Destination must not exist.
    }

    private static async Task<T> ReadJsonAsync<T>(string path, CancellationToken ct)
        where T : class
    {
        await using var input = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<T>(input, cancellationToken: ct)
            ?? throw new InvalidDataException("Invalid metadata");
    }

    // Format: yyyy-MM-dd|event-id|message. Invalid records abort the batch.
    private static DateOnly Parse(string line)
    {
        var parts = line.Split('|', 3);
        if (parts.Length != 3 || string.IsNullOrWhiteSpace(parts[1]) ||
            !DateOnly.TryParseExact(parts[0], "yyyy-MM-dd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var date))
            throw new InvalidDataException("Invalid event record");
        return date;
    }

    private static async Task ValidateAsync(string path, CancellationToken ct)
    {
        using var reader = new StreamReader(path, new UTF8Encoding(false, true));
        while (await reader.ReadLineAsync() is string line)
        {
            ct.ThrowIfCancellationRequested();
            Parse(line);
        }
    }

    public async Task ArchiveAsync(DateOnly cutoff, CancellationToken ct = default)
    {
        await gate.WaitAsync(ct);
        try
        {
            await RecoverCoreAsync(ct);
            string original = await HashAsync(Active, ct);
            bool hasOld = false;
            using (var reader = new StreamReader(Active, new UTF8Encoding(false, true)))
            await using (var oldStream = Create(OldTemp))
            await using (var newStream = Create(NewTemp))
            await using (var oldWriter = new StreamWriter(oldStream, leaveOpen: true))
            await using (var newWriter = new StreamWriter(newStream, leaveOpen: true))
            {
                while (await reader.ReadLineAsync() is string line)
                {
                    ct.ThrowIfCancellationRequested();
                    if (Parse(line) < cutoff)
                    {
                        hasOld = true;
                        await oldWriter.WriteLineAsync(line);
                    }
                    else await newWriter.WriteLineAsync(line);
                }
                await oldWriter.FlushAsync();
                await newWriter.FlushAsync();
                oldStream.Flush(true);
                newStream.Flush(true);
            }
            if (!hasOld) { File.Delete(OldTemp); File.Delete(NewTemp); return; }
            var job = new Job(Guid.NewGuid().ToString("N"), original,
                await HashAsync(OldTemp, ct), await HashAsync(NewTemp, ct));
            await PublishJsonAsync(Journal, job, ct);
            await RecoverCoreAsync(ct);
        }
        finally { gate.Release(); }
    }

    public async Task RecoverAsync(CancellationToken ct = default)
    {
        await gate.WaitAsync(ct);
        try { await RecoverCoreAsync(ct); }
        finally { gate.Release(); }
    }

    private async Task RecoverCoreAsync(CancellationToken ct)
    {
        if (!File.Exists(Journal))
        {
            File.Delete(OldTemp);
            File.Delete(NewTemp);
            File.Delete(Journal + ".tmp");
            return;
        }
        var job = await ReadJsonAsync<Job>(Journal, ct);
        if (!Guid.TryParseExact(job.Id, "N", out _))
            throw new InvalidDataException("Invalid job ID");
        string final = Archive(job.Id);
        string activeHash = await HashAsync(Active, ct);
        if (activeHash != job.Original && activeHash != job.Remaining)
            throw new InvalidDataException("Unexpected active version");
        if (!File.Exists(final))
        {
            await VerifyAsync(OldTemp, job.Old, ct);
            File.Move(OldTemp, final);
        }
        await VerifyAsync(final, job.Old, ct);
        if (activeHash != job.Remaining)
        {
            await VerifyAsync(NewTemp, job.Remaining, ct);
            File.Replace(NewTemp, Active, null);
        }
        await VerifyAsync(Active, job.Remaining, ct);
        File.Delete(OldTemp);
        File.Delete(NewTemp);
        File.Delete(Journal); // Last: retry can reconcile until this point.
    }

    // Backup is a versioned snapshot of ACTIVE data only, not the archive.
    public async Task<string> BackupAsync(CancellationToken ct = default)
    {
        await gate.WaitAsync(ct);
        try
        {
            await RecoverCoreAsync(ct);
            await ValidateAsync(Active, ct);
            string id = Guid.NewGuid().ToString("N");
            string data = Path.Combine(backups, id + ".txt");
            string temp = data + ".tmp";
            string hash = await HashAsync(Active, ct);
            await CopyAsync(Active, temp, ct);
            await VerifyAsync(temp, hash, ct);
            File.Move(temp, data);
            // Manifest is the commit marker; incomplete versions are ignored.
            await PublishJsonAsync(Path.Combine(backups, id + ".json"),
                new BackupInfo(id, hash), ct);
            return id;
        }
        finally { gate.Release(); }
    }

    // Restore to a NEW location for inspection; never overwrite live data.
    // Restoring an older active snapshot over current data could duplicate
    // archived events. Full-system restore needs a matching archive snapshot.
    public async Task<string> RestoreAsync(string id, CancellationToken ct = default)
    {
        if (!Guid.TryParseExact(id, "N", out _)) throw new ArgumentException("Invalid ID");
        await gate.WaitAsync(ct);
        try
        {
            var info = await ReadJsonAsync<BackupInfo>(Path.Combine(backups, id + ".json"), ct);
            if (info.Id != id) throw new InvalidDataException("Wrong backup identity");
            string data = Path.Combine(backups, id + ".txt");
            string target = Path.Combine(root, $"restored-{Guid.NewGuid():N}.txt");
            string temp = target + ".tmp";
            try
            {
                await CopyAsync(data, temp, ct);
                await VerifyAsync(temp, info.Hash, ct);
                await ValidateAsync(temp, ct);
                File.Move(temp, target);
                return target;
            }
            finally { File.Delete(temp); }
        }
        finally { gate.Release(); }
    }
}
