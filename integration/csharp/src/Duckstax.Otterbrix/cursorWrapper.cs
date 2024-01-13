namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class CursorWrapper {
        const string libotterbrix = "libotterbrix.so";

        [StructLayout(LayoutKind.Sequential)]
        private struct TransferErrorMessage {
            public int type;
            public IntPtr what;
        }

        [DllImport(libotterbrix, EntryPoint="release_cursor", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseCursor(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_size", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern int CursorSize(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_has_next", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorHasNext(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_next", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorNext(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_get", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGet(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_get_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr CursorGet(IntPtr ptr, int index);

        [DllImport(libotterbrix, EntryPoint="cursor_is_success", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsSuccess(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="cursor_is_error", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool CursorIsError(IntPtr ptr);

        [DllImport(libotterbrix,
                   EntryPoint = "cursor_get_error",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern TransferErrorMessage CursorGetError(IntPtr ptr);

        public CursorWrapper(IntPtr cursorStoragePtr) { this.cursorStoragePtr = cursorStoragePtr; }
        ~CursorWrapper() { ReleaseCursor(cursorStoragePtr); }
        public int Size() { return CursorSize(cursorStoragePtr); }
        public bool HasNext() { return CursorHasNext(cursorStoragePtr); }
        public DocumentWrapper Next() { return new DocumentWrapper(CursorNext(cursorStoragePtr)); }
        public DocumentWrapper Get() { return new DocumentWrapper(CursorGet(cursorStoragePtr)); }
        public DocumentWrapper Get(int index) { return new DocumentWrapper(CursorGet(cursorStoragePtr, index)); }
        public bool IsSuccess() { return CursorIsSuccess(cursorStoragePtr); }
        public bool IsError() { return CursorIsError(cursorStoragePtr); }
        public ErrorMessage GetError() {
            TransferErrorMessage transfer = CursorGetError(cursorStoragePtr);
            ErrorMessage message = new ErrorMessage();
            message.type = (ErrorCode)transfer.type;
            string? str = Marshal.PtrToStringAnsi(transfer.what);
            message.what = str == null ? "" : str;
            Marshal.FreeHGlobal(transfer.what);
            return message;
        }

        private readonly IntPtr cursorStoragePtr;
    }
}