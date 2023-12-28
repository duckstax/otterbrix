namespace Duckstax.EntityFramework.Otterbrix
{
    using System;
    using System.Runtime.InteropServices;

    public class documentWrapper
    {
        const string libotterbrix = "../../../libotterbrix.so";
        
        [DllImport(libotterbrix, EntryPoint="DocumentID", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern string_view_t ReleaseDocument(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="ReleaseDocument", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern string_view_t DocumentID(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="DocumentIsValid", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsValid(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="DocumentIsArray", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsArray(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="DocumentIsDict", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDict(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="DocumentCount", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern int DocumentCount(IntPtr ptr);
        [DllImport(libotterbrix, EntryPoint="DocumentIsExistByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsExistByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsExistByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsExistByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsNullByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsNullByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsNullByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsNullByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsBoolByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsBoolByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsBoolByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsBoolByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsUlongByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsUlongByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsUlongByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsUlongByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsLongByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsLongByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsLongByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsLongByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsDoubleByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDoubleByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsDoubleByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDoubleByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsStringByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsStringByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsStringByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsStringByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsArrayByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsArrayByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsArrayByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsArrayByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentIsDictByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDictByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentIsDictByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentIsDictByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentGetBoolByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentGetBoolByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentGetBoolByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern bool DocumentGetBoolByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentGetUlongByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern ulong DocumentGetUlongByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentGetUlongByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern ulong DocumentGetUlongByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentGetLongByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern long DocumentGetLongByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentGetLongByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern long DocumentGetLongByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentGetDoubleByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern double DocumentGetDoubleByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentGetDoubleByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern double DocumentGetDoubleByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentGetStringByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern string_view_t DocumentGetStringByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentGetStringByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern string_view_t DocumentGetStringByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentGetArrayByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetArrayByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentGetArrayByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetArrayByIndex(IntPtr ptr, int index);
        [DllImport(libotterbrix, EntryPoint="DocumentGetDictByKey", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetDictByKey(IntPtr ptr, string_view_t key);
        [DllImport(libotterbrix, EntryPoint="DocumentGetDictByIndex", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern IntPtr DocumentGetDictByIndex(IntPtr ptr, int index);
        public documentWrapper(IntPtr doc_ptr)
        {
            doc_ptr_ = doc_ptr;
        }
        ~documentWrapper()
        {
            ReleaseDocument(doc_ptr_);
        }
        public string ID()
        {
            return DocumentID(doc_ptr_).data;
        }
        public bool IsValid()
        {
            return DocumentIsValid(doc_ptr_);
        }
        public bool IsArray()
        {
            return DocumentIsArray(doc_ptr_);
        }
        public bool IsDict()
        {
            return DocumentIsDict(doc_ptr_);
        }
        public int Count()
        {
            return DocumentCount(doc_ptr_);
        }
        public bool IsExist(string key)
        {
            return DocumentIsExistByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsExist(int index)
        {
            return DocumentIsExistByIndex(doc_ptr_, index);
        }
        public bool IsNull(string key)
        {
            return DocumentIsNullByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsNull(int index)
        {
            return DocumentIsNullByIndex(doc_ptr_, index);
        }
        public bool IsBool(string key)
        {
            return DocumentIsBoolByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsBool(int index)
        {
            return DocumentIsBoolByIndex(doc_ptr_, index);
        }
        public bool IsUlong(string key)
        {
            return DocumentIsUlongByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsUlong(int index)
        {
            return DocumentIsUlongByIndex(doc_ptr_, index);
        }
        public bool IsLong(string key)
        {
            return DocumentIsLongByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsLong(int index)
        {
            return DocumentIsLongByIndex(doc_ptr_, index);
        }
        public bool IsDouble(string key)
        {
            return DocumentIsDoubleByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsDouble(int index)
        {
            return DocumentIsDoubleByIndex(doc_ptr_, index);
        }
        public bool IsString(string key)
        {
            return DocumentIsStringByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsString(int index)
        {
            return DocumentIsStringByIndex(doc_ptr_, index);
        }
        public bool IsArray(string key)
        {
            return DocumentIsArrayByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsArray(int index)
        {
            return DocumentIsArrayByIndex(doc_ptr_, index);
        }
        public bool IsDict(string key)
        {
            return DocumentIsDictByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool IsDict(int index)
        {
            return DocumentIsDictByIndex(doc_ptr_, index);
        }
        public bool GetBool(string key)
        {
            return DocumentGetBoolByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public bool GetBool(int index)
        {
            return DocumentGetBoolByIndex(doc_ptr_, index);
        }
        public ulong GetUlong(string key)
        {
            return DocumentGetUlongByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public ulong GetUlong(int index)
        {
            return DocumentGetUlongByIndex(doc_ptr_, index);
        }
        public long GetLong(string key)
        {
            return DocumentGetLongByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public long GetLong(int index)
        {
            return DocumentGetLongByIndex(doc_ptr_, index);
        }
        public double GetDouble(string key)
        {
            return DocumentGetDoubleByKey(doc_ptr_, stringConverter.ToStringView(ref key));
        }
        public double GetDouble(int index)
        {
            return DocumentGetDoubleByIndex(doc_ptr_, index);
        }
        public string GetString(string key)
        {
            return DocumentGetStringByKey(doc_ptr_, stringConverter.ToStringView(ref key)).data;
        }
        public string GetString(int index)
        {
            return DocumentGetStringByIndex(doc_ptr_, index).data;
        }
        public documentWrapper GetArray(string key)
        {
            return new documentWrapper(DocumentGetArrayByKey(doc_ptr_, stringConverter.ToStringView(ref key)));
        }
        public documentWrapper GetArray(int index)
        {
            return new documentWrapper(DocumentGetArrayByIndex(doc_ptr_, index));
        }
        public documentWrapper GetDict(string key)
        {
            return new documentWrapper(DocumentGetDictByKey(doc_ptr_, stringConverter.ToStringView(ref key)));
        }
        public documentWrapper GetDict(int index)
        {
            return new documentWrapper(DocumentGetDictByIndex(doc_ptr_, index));
        }
        
        private readonly IntPtr doc_ptr_;
    }
}