namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    [StructLayout(LayoutKind.Sequential)]
    public struct StringPasser
    {
        [MarshalAs(UnmanagedType.LPStr)] public string data;
        public uint size;
        public StringPasser(ref string str)
        {
            data = str;
            size = (uint)str.Length;
        }
    }

    public enum ErrorCode: int 
    {
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

    public struct ErrorMessage
    {
        public ErrorCode type;
        public string what;
    }

    public struct Config
    {
        public enum LogLevel: int
        {
            Trace = 0,
            Debug = 1,
            Info = 2,
            Warn = 3,
            Err = 4,
            Critical = 5,
            Off = 6,
        }
        public LogLevel level;
        public string log_path;
        public string wal_path;
        public string disk_path;
        public bool wal_on;
        public bool disk_on;
        public bool sync_wal_to_disk;

        public Config()
        {
            level = LogLevel.Trace;
            log_path = System.Environment.CurrentDirectory + "/log";
            wal_path = System.Environment.CurrentDirectory + "/wal";
            disk_path = System.Environment.CurrentDirectory + "/disk";
            wal_on = true;
            disk_on = true;
            sync_wal_to_disk = true;
        }
        public Config(LogLevel level, string log_path, string wal_path, string disk_path, bool wal, bool disk, bool wal_disk_sync)
        {
            this.level = level;
            this.log_path = log_path;
            this.wal_path = wal_path;
            this.disk_path = disk_path;
            wal_on = wal;
            disk_on = disk;
            sync_wal_to_disk = wal_disk_sync;
        }
        public static Config DefaultConfig()
        {
            return new Config();
        }
    }
    
    public class OtterbrixWrapper
    {

        const string libotterbrix = "../../../libotterbrix.so";
        
        [StructLayout(LayoutKind.Sequential)]
        private struct TransferConfig
        {
            public int level;
            public StringPasser log_path;
            public StringPasser wal_path;
            public StringPasser disk_path;
            public bool wal_on;
            public bool disk_on;
            public bool sync_wal_to_disk;
            public TransferConfig(ref Config config)
            {
                this.level = (int)config.level;
                this.log_path = new StringPasser(ref config.log_path);
                this.wal_path = new StringPasser(ref config.wal_path);
                this.disk_path = new StringPasser(ref config.disk_path);
                this.wal_on = config.wal_on;
                this.disk_on = config.disk_on;
                this.sync_wal_to_disk = config.sync_wal_to_disk;
            }
        }
        
        [DllImport(libotterbrix, EntryPoint="otterbrix_create", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr otterbrix_create(TransferConfig config, StringPasser database, StringPasser collection);
        [DllImport(libotterbrix, EntryPoint="otterbrix_destroy", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void otterbrix_destroy(IntPtr otterprix_ptr);
        [DllImport(libotterbrix, EntryPoint="execute_sql", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr execute_sql(IntPtr otterprix_ptr, StringPasser sql);

        public OtterbrixWrapper(Config config, string database, string collection)
        {
            otterbrix_ptr_ = otterbrix_create(new TransferConfig(ref config), new StringPasser(ref database), new StringPasser(ref collection));
        }
        ~OtterbrixWrapper()
        {
            otterbrix_destroy(otterbrix_ptr_);
        }
        public CursorWrapper Execute(string sql)
        {
            return new CursorWrapper(execute_sql(otterbrix_ptr_, new StringPasser(ref sql)));
        }

        private readonly IntPtr otterbrix_ptr_;
    }
}