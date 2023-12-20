namespace Duckstax.EntityFramework.otterbrix
{
    using System.Runtime.InteropServices;

    public class otterbrixWrapper
    {
        [DllImport("otterbrix.so", EntryPoint="execute_sql", ExactSpelling=false,CallingConvention=CallingConvention.Cdecl)]
        private static extern int execute_sql( string sql);

        public bool Execute()
        {
            // TODO: Execute whatever
            return true;
        }
    }
}