namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class DocumentWrapper {
        const string libotterbrix = "libotterbrix.so";
        
        [DllImport(libotterbrix, EntryPoint="document_id", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentID(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="release_document", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern void ReleaseDocument(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="document_is_valid", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsValid(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="document_is_array", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsArray(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="document_is_dict", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDict(IntPtr ptr);

        [DllImport(libotterbrix, EntryPoint="document_count", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern int DocumentCount(IntPtr ptr);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_exist_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsExistByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_exist_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsExistByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_null_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsNullByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_null_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsNullByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_bool_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsBoolByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_bool_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsBoolByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_ulong_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsUlongByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_ulong_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsUlongByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_long_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsLongByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_long_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsLongByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_double_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsDoubleByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_double_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDoubleByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_string_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsStringByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_string_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsStringByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_array_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsArrayByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_array_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsArrayByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_is_dict_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentIsDictByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_is_dict_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDictByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_get_bool_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern bool DocumentGetBoolByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_get_bool_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentGetBoolByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_get_ulong_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern ulong DocumentGetUlongByKey(IntPtr ptr, StringPasser key);


        [DllImport(libotterbrix, EntryPoint="document_get_ulong_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern ulong DocumentGetUlongByIndex(IntPtr ptr, int index);


        [DllImport(libotterbrix,
                   EntryPoint = "document_get_long_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern long DocumentGetLongByKey(IntPtr ptr, StringPasser key);
        [DllImport(libotterbrix, EntryPoint="document_get_long_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern long DocumentGetLongByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_get_double_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern double DocumentGetDoubleByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_get_double_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern double DocumentGetDoubleByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_get_string_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetStringByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_get_string_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetStringByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_get_array_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetArrayByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_get_array_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetArrayByIndex(IntPtr ptr, int index);

        [DllImport(libotterbrix,
                   EntryPoint = "document_get_dict_by_key",
                   ExactSpelling = false,
                   CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetDictByKey(IntPtr ptr, StringPasser key);

        [DllImport(libotterbrix, EntryPoint="document_get_dict_by_index", ExactSpelling=false, CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetDictByIndex(IntPtr ptr, int index);

        
        public DocumentWrapper(IntPtr docPtr) { this.docPtr = docPtr; }
        ~DocumentWrapper() { ReleaseDocument(docPtr); }
        public string ID()
        {
            IntPtr strPtr = DocumentID(docPtr);
            string? result = Marshal.PtrToStringAnsi(strPtr);
            Marshal.FreeHGlobal(strPtr);
            return result == null ? "" : result;
        }
        public bool IsValid() { return DocumentIsValid(docPtr); }
        public bool IsArray() { return DocumentIsArray(docPtr); }
        public bool IsDict() { return DocumentIsDict(docPtr); }
        public int Count() { return DocumentCount(docPtr); }
        public bool IsExist(string key) { return DocumentIsExistByKey(docPtr, new StringPasser(ref key)); }
        public bool IsExist(int index) { return DocumentIsExistByIndex(docPtr, index); }
        public bool IsNull(string key) { return DocumentIsNullByKey(docPtr, new StringPasser(ref key)); }
        public bool IsNull(int index) { return DocumentIsNullByIndex(docPtr, index); }
        public bool IsBool(string key) { return DocumentIsBoolByKey(docPtr, new StringPasser(ref key)); }
        public bool IsBool(int index) { return DocumentIsBoolByIndex(docPtr, index); }
        public bool IsUlong(string key) { return DocumentIsUlongByKey(docPtr, new StringPasser(ref key)); }
        public bool IsUlong(int index) { return DocumentIsUlongByIndex(docPtr, index); }
        public bool IsLong(string key) { return DocumentIsLongByKey(docPtr, new StringPasser(ref key)); }
        public bool IsLong(int index) { return DocumentIsLongByIndex(docPtr, index); }
        public bool IsDouble(string key) { return DocumentIsDoubleByKey(docPtr, new StringPasser(ref key)); }
        public bool IsDouble(int index) { return DocumentIsDoubleByIndex(docPtr, index); }
        public bool IsString(string key) { return DocumentIsStringByKey(docPtr, new StringPasser(ref key)); }
        public bool IsString(int index) { return DocumentIsStringByIndex(docPtr, index); }
        public bool IsArray(string key) { return DocumentIsArrayByKey(docPtr, new StringPasser(ref key)); }
        public bool IsArray(int index) { return DocumentIsArrayByIndex(docPtr, index); }
        public bool IsDict(string key) { return DocumentIsDictByKey(docPtr, new StringPasser(ref key)); }
        public bool IsDict(int index) { return DocumentIsDictByIndex(docPtr, index); }
        public bool GetBool(string key) { return DocumentGetBoolByKey(docPtr, new StringPasser(ref key)); }
        public bool GetBool(int index) { return DocumentGetBoolByIndex(docPtr, index); }
        public ulong GetUlong(string key) { return DocumentGetUlongByKey(docPtr, new StringPasser(ref key)); }
        public ulong GetUlong(int index) { return DocumentGetUlongByIndex(docPtr, index); }
        public long GetLong(string key) { return DocumentGetLongByKey(docPtr, new StringPasser(ref key)); }
        public long GetLong(int index) { return DocumentGetLongByIndex(docPtr, index); }
        public double GetDouble(string key) { return DocumentGetDoubleByKey(docPtr, new StringPasser(ref key)); }
        public double GetDouble(int index) { return DocumentGetDoubleByIndex(docPtr, index); }
        public string GetString(string key)
        {
            IntPtr strPtr = DocumentGetStringByKey(docPtr, new StringPasser(ref key));
            string? result = Marshal.PtrToStringAnsi(strPtr);
            Marshal.FreeHGlobal(strPtr);
            return result == null ? "" : result;
        }
        public string GetString(int index)
        {
            IntPtr strPtr = DocumentGetStringByIndex(docPtr, index);
            string? result = Marshal.PtrToStringAnsi(strPtr);
            Marshal.FreeHGlobal(strPtr);
            return result == null ? "" : result;
        }
        public DocumentWrapper GetArray(string key) {
            return new DocumentWrapper(DocumentGetArrayByKey(docPtr, new StringPasser(ref key)));
        }
        public DocumentWrapper GetArray(int index) {
            return new DocumentWrapper(DocumentGetArrayByIndex(docPtr, index));
        }
        public DocumentWrapper GetDict(string key) {
            return new DocumentWrapper(DocumentGetDictByKey(docPtr, new StringPasser(ref key)));
        }
        public DocumentWrapper GetDict(int index) { return new DocumentWrapper(DocumentGetDictByIndex(docPtr, index)); }

        private readonly IntPtr docPtr;
    }
}