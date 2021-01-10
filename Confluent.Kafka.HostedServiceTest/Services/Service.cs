using System;

namespace TestApplication.Services
{
    public class Service : IService
    {
        public void WriteToConsole(string arg)
        {
            Console.WriteLine(arg);
        }
    }
}