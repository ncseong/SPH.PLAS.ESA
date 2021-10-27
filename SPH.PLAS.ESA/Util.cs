using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace SPH.PLAS.ESA
{
    public class Util
    {
        public static byte[] ConvertToByteArray(int [] array)
        {
            byte[] result = new byte[array.Length * 4];
            for(int i = 0; i<array.Length; i++)
            {
                byte[] tmp = BitConverter.GetBytes(array[i]);
                Array.Copy(tmp, 0, result, i * 4, 4);
            }
            return result;
        }

        public static int[] ConvertToIntArray(byte[] array)
        {
            int[] result = new int[array.Length / 4];
            for(int i = 0; i<array.Length; i+=4)
            {
                result[i / 4] = BitConverter.ToInt32(array, i);
            }
            return result;
        }

        public static byte[] ConvertToByteArray(IDictionary<int,int> dictionary)
        {
            var binFormatter = new BinaryFormatter();
            var mStream = new MemoryStream();
            binFormatter.Serialize(mStream, dictionary);

            //This gives you the byte array.
            return mStream.ToArray();
        }

        public static byte[] ConvertToByteArray(IDictionary<int, IList<int>> dictionary)
        {
            var binFormatter = new BinaryFormatter();
            var mStream = new MemoryStream();
            binFormatter.Serialize(mStream, dictionary);

            //This gives you the byte array.
            return mStream.ToArray();
        }

        public static IDictionary<int, IList<int>> ConvertToDictionary(byte[] dictionary)
        {
            var mStream = new MemoryStream();
            var binFormatter = new BinaryFormatter();

            // Where 'objectBytes' is your byte array.
            mStream.Write(dictionary, 0, dictionary.Length);
            mStream.Position = 0;

            return binFormatter.Deserialize(mStream) as IDictionary<int,IList<int>>;
        }
        public static byte[] ConvertToByteArray(HashSet<int> hashSet)
        {
            var binFormatter = new BinaryFormatter();
            var mStream = new MemoryStream();
            binFormatter.Serialize(mStream, hashSet);

            //This gives you the byte array.
            return mStream.ToArray();
        }

        public static HashSet<int> ConvertToHashSet(byte[] hashSet)
        {
            var mStream = new MemoryStream();
            var binFormatter = new BinaryFormatter();

            // Where 'objectBytes' is your byte array.
            mStream.Write(hashSet, 0, hashSet.Length);
            mStream.Position = 0;

            return binFormatter.Deserialize(mStream) as HashSet<int>;
        }

        public static void CleanUDB(DateTime now, string path, int udbLifePeriod, string name)
        {
            string today = now.ToString("yyyyMMdd");
            if (!path.EndsWith("\\"))
            {
                path += "\\";
            }
            string backupPath = string.Format("{0}backup\\{1}\\{2}", path, name, today);
            if (!Directory.Exists(backupPath))
            {
                Directory.CreateDirectory(backupPath);
            }
            path += name;
            string[] files = Directory.GetFiles(path, "*.udbx", SearchOption.AllDirectories);
            foreach (string filePath in files)
            {
                if (File.GetCreationTime(filePath) < now)
                {
                    string fileName = filePath.Substring(filePath.LastIndexOf("\\"));
                    try
                    {
                        File.Move(filePath, string.Format("{0}{1}", backupPath, fileName));
                    }
                    catch (Exception e) {
                        throw e;
                    }
                }
            }


            string[] dirs = Directory.GetDirectories(backupPath.Substring(0, backupPath.LastIndexOf("\\")));
            foreach (string dir in dirs)
            {
                if (Directory.GetCreationTime(dir).AddDays(udbLifePeriod) <= now)
                {
                    Directory.Delete(dir, true);
                }
            }
        }
    }
}
