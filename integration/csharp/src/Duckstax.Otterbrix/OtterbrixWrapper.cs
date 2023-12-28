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
    [StructLayout(LayoutKind.Sequential)]
    public struct error_message
    {
        public int error_code;
        public string_view_t message;
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
    public static class stringConverter
    {
        public static string_view_t ToStringView(ref string str)
        {
            string_view_t str_view;
            str_view.data = str;
            str_view.size = (uint)str.Length;
            return str_view;
        }
    }
    public class otterbrixWrapper
    {
        const string libotterbrix = "../../../libotterbrix.so";
        
        [DllImport(libotterbrix, EntryPoint="otterbrix_create", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr otterbrix_create();
        [DllImport(libotterbrix, EntryPoint="otterbrix_destroy", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern void otterbrix_destroy(IntPtr otterprix_ptr);
        [DllImport(libotterbrix, EntryPoint="execute_sql", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr execute_sql(IntPtr otterprix_ptr, string_view_t sql);

        public otterbrixWrapper()
        {
            otterbrix_ptr_ = otterbrix_create();
        }
        ~otterbrixWrapper()
        {
            otterbrix_destroy(otterbrix_ptr_);
        }
        public cursorWrapper Execute(string sql)
        {
            return new cursorWrapper(execute_sql(otterbrix_ptr_, stringConverter.ToStringView(ref sql)));
        }

        private readonly IntPtr otterbrix_ptr_;
    }
}