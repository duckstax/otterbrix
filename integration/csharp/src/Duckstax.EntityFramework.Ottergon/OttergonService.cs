namespace Duckstax.EntityFramework.Ottergon
{
    using System.Runtime.InteropServices;

    public class OttergonService
    {
        [DllImport("ottergon.so", EntryPoint="execute_sql", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        public static extern int execute_sql( string sql);

        public static void Main(string[] args)
        {

        }
    }
}