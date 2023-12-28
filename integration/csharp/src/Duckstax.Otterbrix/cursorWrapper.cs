namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class cursorWrapper
    {
        const string libotterbrix = "../../../libotterbrix.so";
        
        [DllImport(libotterbrix, EntryPoint="ReleaseCursor", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseCursor(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorSize", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorSize(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorHasNext", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorHasNext(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorNext", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorNext(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorGet", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGet(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorGetByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGet(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="CursorIsSuccess", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsSuccess(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorIsError", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsError(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="CursorGetError", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern error_message CursorGetError(IntPtr ptr);

        public cursorWrapper(IntPtr cursor_storage_ptr)
        {
            cursor_storage_ptr_ = cursor_storage_ptr;
        }
        ~cursorWrapper()
        {
            ReleaseCursor(cursor_storage_ptr_);
        }
        public int Size()
        {
            return CursorSize(cursor_storage_ptr_);
        }
        public bool HasNext()
        {
            return CursorHasNext(cursor_storage_ptr_);
        }
        public documentWrapper Next()
        {
            return new documentWrapper(CursorNext(cursor_storage_ptr_));
        }
        public documentWrapper Get()
        {
            return new documentWrapper(CursorGet(cursor_storage_ptr_));
        }
        public documentWrapper Get(int index)
        {
            return new documentWrapper(CursorGet(cursor_storage_ptr_, index));
        }
        public bool IsSuccess()
        {
            return CursorIsSuccess(cursor_storage_ptr_);
        }
        public bool IsError()
        {
            return CursorIsError(cursor_storage_ptr_);
        }
        public error_message GetError()
        {
            return CursorGetError(cursor_storage_ptr_);
        }
        
        private readonly IntPtr cursor_storage_ptr_;
    }
}