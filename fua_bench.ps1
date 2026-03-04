# Standalone FUA latency benchmark using PowerShell + .NET
# Measures raw FILE_FLAG_WRITE_THROUGH write latency on H: and C: drives

$code = @"
using System;
using System.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

public class FuaBench {
    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
    static extern SafeFileHandle CreateFile(
        string lpFileName, uint dwDesiredAccess, uint dwShareMode,
        IntPtr lpSecurityAttributes, uint dwCreationDisposition,
        uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    const uint GENERIC_WRITE = 0x40000000;
    const uint OPEN_ALWAYS = 4;
    const uint FILE_FLAG_WRITE_THROUGH = 0x80000000;

    public static void Run(string path, int iterations) {
        var handle = CreateFile(path, GENERIC_WRITE, 0, IntPtr.Zero, OPEN_ALWAYS, FILE_FLAG_WRITE_THROUGH, IntPtr.Zero);
        if (handle.IsInvalid) { Console.WriteLine("Failed to open: " + Marshal.GetLastWin32Error()); return; }
        var fs = new FileStream(handle, FileAccess.Write);
        var data = new byte[200]; // typical WAL record size
        new Random(42).NextBytes(data);
        
        // warmup
        for (int i = 0; i < 100; i++) fs.Write(data, 0, data.Length);
        
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++) {
            fs.Write(data, 0, data.Length);
        }
        sw.Stop();
        
        double totalMs = sw.Elapsed.TotalMilliseconds;
        double avgUs = totalMs / iterations * 1000;
        double tps = iterations / (totalMs / 1000);
        Console.WriteLine($"  {path}: {iterations} writes in {totalMs:F1}ms, avg={avgUs:F1}us, max_tps={tps:F0}");
        
        fs.Close();
        File.Delete(path);
    }
}
"@

Add-Type -TypeDefinition $code -Language CSharp

$n = 5000
Write-Host "FUA write latency benchmark ($n iterations, 200 bytes each):"
[FuaBench]::Run("H:\fua_test.bin", $n)
[FuaBench]::Run("C:\fua_test.bin", $n)
