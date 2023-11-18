using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Ottergon ///PINVOKE
{
    public static class OttergonProgram
    {

        [DllImport("ottergon.so", EntryPoint="execute_sql", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        public static extern int execute_sql( string sql);

        public static void Main(string[] args)
        {

        }
    }

}