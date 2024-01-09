namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    [StructLayout(LayoutKind.Sequential)]
    public struct StringPasser {
        [MarshalAs(UnmanagedType.LPStr)] public string data;
        public uint size;
        public StringPasser(ref string str) {
            data = str;
            size = (uint) str.Length;
        }
    }

    public enum ErrorCode : int {
        None = 0,
        DatabaseAlreadyExists = 1,
        DatabaseNotExists = 2,
        CollectionAlreadyExists = 3,
        CollectionNotExists = 4,
        CollectionDropped = 5,
        SqlParseError = 6,
        CreatePhisicalPlanError = 7,
        OtherError = -1
    }

    public struct ErrorMessage {
        public ErrorCode type;
        public string what;
    }

    public struct Config {
        public enum LogLevel : int {
            Trace = 0,
            Debug = 1,
            Info = 2,
            Warn = 3,
            Err = 4,
            Critical = 5,
            Off = 6,
        }
        public LogLevel level;
        public string logPath;
        public string walPath;
        public string diskPath;
        public bool walOn;
        public bool diskOn;
        public bool syncWalToDisk;

        public Config() {
            level = LogLevel.Trace;
            logPath = System.Environment.CurrentDirectory + "/log";
            walPath = System.Environment.CurrentDirectory + "/wal";
            diskPath = System.Environment.CurrentDirectory + "/disk";
            walOn = true;
            diskOn = true;
            syncWalToDisk = true;
        }
        public Config(LogLevel level,
                      string logPath,
                      string walPath,
                      string diskPath,
                      bool wal,
                      bool disk,
                      bool walDiskSync) {
            this.level = level;
            this.logPath = logPath;
            this.walPath = walPath;
            this.diskPath = diskPath;
            walOn = wal;
            diskOn = disk;
            syncWalToDisk = walDiskSync;
        }
        public static Config DefaultConfig() { return new Config(); }
    }

    public class OtterbrixWrapper {
        const string libotterbrix = "libotterbrix.so";

        [StructLayout(LayoutKind.Sequential)]
        private struct TransferConfig {
            public int level;
            public StringPasser logPath;
            public StringPasser walPath;
            public StringPasser diskPath;
            public bool walOn;
            public bool diskOn;
            public bool syncWalToDisk;
            public TransferConfig(ref Config config) {
                this.level = (int) config.level;
                this.logPath = new StringPasser(ref config.logPath);
                this.walPath = new StringPasser(ref config.walPath);
                this.diskPath = new StringPasser(ref config.diskPath);
                this.walOn = config.walOn;
                this.diskOn = config.diskOn;
                this.syncWalToDisk = config.syncWalToDisk;
            }
        }

        [DllImport(libotterbrix,
                   EntryPoint = "otterbrix_create",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr
        OtterbrixCreate(TransferConfig config, StringPasser database, StringPasser collection);

        [DllImport(libotterbrix,
                   EntryPoint = "otterbrix_destroy",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern void OtterbrixDestroy(IntPtr otterprixPtr);
        
        [DllImport(libotterbrix,
                   EntryPoint = "execute_sql",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ExecuteSQL(IntPtr otterprixPtr, StringPasser sql);

        public OtterbrixWrapper(Config config, string database, string collection) {
            otterbrixPtr = OtterbrixCreate(new TransferConfig(ref config),
                                           new StringPasser(ref database),
                                           new StringPasser(ref collection));
        }
        ~OtterbrixWrapper() { OtterbrixDestroy(otterbrixPtr); }
        public CursorWrapper Execute(string sql) {
            return new CursorWrapper(ExecuteSQL(otterbrixPtr, new StringPasser(ref sql)));
        }

        private readonly IntPtr otterbrixPtr;
    }
}