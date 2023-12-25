namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;


    public class cursorWrapper
    {
        [DllImport("../../../libotterbrix.so", EntryPoint="RecastCursor", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr RecastCursor(IntPtr ptr);
        [DllImport("../../../libotterbrix.so", EntryPoint="ReleaseCursor", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseCursor(IntPtr ptr);
        [DllImport("../../../libotterbrix.so", EntryPoint="CursorSize", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorSize(IntPtr ptr);

        public cursorWrapper(IntPtr cursor_storage_ptr)
        {
            cursor_storage = RecastCursor(cursor_storage_ptr);
        }
        public int Size()
        {
            return CursorSize(cursor_storage);
        }
        ~cursorWrapper()
        {
            ReleaseCursor(cursor_storage);
        }

        
        private readonly IntPtr cursor_storage;
    }
}