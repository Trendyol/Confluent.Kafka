using System;

namespace TestApplication
{
    public class Service : IService
    {
        public void WriteToConsole(string arg)
        {
            Console.WriteLine(arg);
        }
    }
}