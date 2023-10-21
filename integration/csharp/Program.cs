// See https://aka.ms/new-console-template for more information
// Console.WriteLine("Hello, World!");
// Console.Write("Put something -> ");
// var tmp = Console.ReadLine();
// Console.WriteLine($"Your put this data: {tmp}");

using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace PInvokeSamples
{
    public static class Program
    {
        // [DllImport("libmytest.so", EntryPoint="ret_five", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        // private static extern int ret_five();

        // public static void Main(string[] args)
        // {
        //     // Invoke the function and get the process ID.
        //     int ret = ret_five();
        //     Console.WriteLine(ret);
        // }

        [DllImport("libc.so.6")]
        private static extern int getpid();

        [DllImport("libmytest.so", EntryPoint="ret_five", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern int ret_five();

        public static void Main(string[] args)
        {
            // Invoke the function and get the process ID.
            int pid = getpid();
            Console.WriteLine(pid);

            int ret = ret_five();
            Console.WriteLine(ret);
        }
    }

}

// private static IntPtr ImportResolver(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
//         {
//             IntPtr libHandle = IntPtr.Zero;
//             if (libraryName == "libmytest")
//             {
//                 if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
//                 {
//                     libHandle = NativeLibrary.Load("xxxx.dll");
//                 }
//                 else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
//                 {
//                     libHandle = NativeLibrary.Load("xxxx.so");
//                 }
//             }
//             return libHandle;
//         }


        // Import the libc shared library and define the method
        // corresponding to the native function.

        // [DllImport("libmytest", CallingConvention = CallingConvention.Cdecl)]