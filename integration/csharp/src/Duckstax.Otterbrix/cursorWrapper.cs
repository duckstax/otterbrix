namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class cursorWrapper
    {
        const string libotterbrix = "../../../libotterbrix.so";
        
        [DllImport(libotterbrix, EntryPoint="RecastCursor", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr RecastCursor(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="ReleaseCursor", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseCursor(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorSize", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorSize(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorHasNext", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorHasNext(IntPtr ptr);
        /*
        [DllImport(libotterbrix, EntryPoint="CursorNext", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorNext(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorGet", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGet(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorGetByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGet(IntPtr ptr, int index);
        */
        [DllImport(libotterbrix, EntryPoint="CursorIsSuccess", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsSuccess(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorIsError", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsError(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorGetError", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern error_message CursorGetError(IntPtr ptr);

        public cursorWrapper(IntPtr cursor_storage_ptr)
        {
            cursor_storage = RecastCursor(cursor_storage_ptr);
        }
        public int Size()
        {
            return CursorSize(cursor_storage);
        }
        public bool HasNext()
        {
            return CursorHasNext(cursor_storage);
        }
        public bool IsSuccess()
        {
            return CursorIsSuccess(cursor_storage);
        }
        public bool IsError()
        {
            return CursorIsError(cursor_storage);
        }
        public error_message GetError()
        {
            return CursorGetError(cursor_storage);
        }
        ~cursorWrapper()
        {
            ReleaseCursor(cursor_storage);
        }
        
        private readonly IntPtr cursor_storage;
    }
}