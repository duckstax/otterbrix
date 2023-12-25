namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    [StructLayout(LayoutKind.Sequential)]
    public struct string_view_t
    {
        [MarshalAs(UnmanagedType.LPStr)] public string data;
        public uint size;
    }

/*
    [StructLayout(LayoutKind.Sequential)]
    struct config_log_t
    {
        string_view_t data;
    }

    [StructLayout(LayoutKind.Sequential)]
    struct config_wal_t
    {
        string_view_t path;
        bool on;
        bool sync_to_disk;
    }
    
    [StructLayout(LayoutKind.Sequential)]
    struct config_disk_t
    {
        string_view_t path;
        bool on;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct config_t
    {
        config_log_t log;
        config_wal_t wal;
        config_disk_t disk;
    }

*/
    public class otterbrixWrapper
    {
        [DllImport("../../../libotterbrix.so", EntryPoint="otterbrix_create", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr otterbrix_create();
        [DllImport("../../../libotterbrix.so", EntryPoint="otterbrix_destroy", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern void otterbrix_destroy(IntPtr otterprix_ptr);
        [DllImport("../../../libotterbrix.so", EntryPoint="execute_sql", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern void execute_sql(IntPtr otterprix_ptr, string_view_t sql);

        public otterbrixWrapper()
        {
            otterbrix_ptr_ = otterbrix_create();
        }
        ~otterbrixWrapper()
        {
            otterbrix_destroy(otterbrix_ptr_);
        }
        public bool Execute(string sql)
        {
            string_view_t str;
            str.data = sql;
            str.size = (uint)sql.Length;
            execute_sql(otterbrix_ptr_, str);
            //cursorWrapper cursor = new cursorWrapper(execute_sql(sql));
            return true;
        }

        private readonly IntPtr otterbrix_ptr_;
    }
}