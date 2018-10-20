using Rebus.Time;
using System;
using System.IO;
using System.Linq;

namespace Rebus.Transport.FileSystem
{
    class TransportHelper
    {
        private static readonly DateTime historicalDate = new DateTime(1970, 1, 1, 0, 0, 0);
        private const int PART1_LENGH = 19 + 5;

        /// <summary>
        /// Generates unique 22 character string
        /// </summary>
        /// <returns></returns>
        public static string GenerateID()
        {
            var charsToRemove = new char[] { '/', '+', '=' };
            var replacement = new char[] { 'a', 'b', 'c' };
            string str = Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Substring(0, 22);
            int len = str.Length;
            for (int i = 0; i < charsToRemove.Length; ++i)
            {
                str = str.Replace(charsToRemove[i], replacement[i]);
            }
            return str;
        }

        public static bool RenameFile(string fullPath, string newFileName, out string newFilePath)
        {
            newFilePath = null;
            try
            {
                string dirName = Path.GetDirectoryName(fullPath);
                string fileName = Path.GetFileName(fullPath);
                newFilePath = Path.Combine(dirName, newFileName);
                FileInfo fileInfo = new FileInfo(fullPath);
                fileInfo.MoveTo(newFilePath);
                return File.Exists(newFilePath);
            }
            catch (IOException)
            {
                // the file is gone
                newFilePath = null;
                return false;
            }
        }

        /// <summary>
        /// change the first letter from any to t (means temporary)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <param name="newFilePath"></param>
        /// <returns></returns>
        public static bool RenameToTemp(string fullPath, out string newFilePath)
        {
            newFilePath = null;
            string fileName = Path.GetFileName(fullPath);
            string newFileName = $"t{fileName.Substring(1)}";
            return RenameFile(fullPath, newFileName, out newFilePath);
        }

        /// <summary>
        /// Double unique rename to secure exclusive file access (one time is not enough)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <param name="newFilePath"></param>
        /// <returns></returns>
        public static bool RenameToUniqueTempName(string fullPath, out string newFilePath)
        {
            if (!TransportHelper._RenameToUniqueTempName(fullPath, out newFilePath))
            {
                return false;
            }
            fullPath = newFilePath;
            if (!TransportHelper._RenameToUniqueTempName(fullPath, out newFilePath))
            {
                return false;
            }
            return true;
        }


        private static bool _RenameToUniqueTempName(string fullPath, out string newFilePath)
        {
            newFilePath = null;
            string fileName = Path.GetFileName(fullPath);
            string newFileName = GetDerivedTempFileName(fileName);
            if (!RenameFile(fullPath, newFileName, out newFilePath))
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// change the first letter from any to e (means error)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <param name="newFilePath"></param>
        /// <returns></returns>
        public static bool RenameToError(string fullPath, out string newFilePath)
        {
            newFilePath = null;
            string fileName = Path.GetFileName(fullPath);
            string newFileName = $"e{fileName.Substring(1)}";
            return RenameFile(fullPath, newFileName, out newFilePath);
        }

        /// <summary>
        /// Gets the date in UTC when the file was sent
        /// </summary>
        /// <param name="fullPath"></param>
        /// <returns></returns>
        public static DateTime GetSendDate(string fullPath)
        {
            string fileName = Path.GetFileName(fullPath);
            long ticks = long.Parse(fileName.Substring(1, 19), System.Globalization.NumberStyles.Number) + historicalDate.Ticks;
            return new DateTime(ticks).ToUniversalTime();
        }

        /// <summary>
        /// Get the DateTime ticks in the form of the padded with '0' string
        /// </summary>
        /// <returns></returns>
        public static string GetTimeTicks()
        {
            return (DateTime.Now.Ticks - TransportHelper.historicalDate.Ticks).ToString().PadLeft(19, '0');
        }

        /// <summary>
        /// Get the age of the file (it is coded in the file name)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <returns></returns>
        public static TimeSpan GetFileAge(string fullPath)
        {
            DateTime sendTimeUtc = TransportHelper.GetSendDate(fullPath);
            DateTime nowUtc = RebusTime.Now.UtcDateTime;

           return nowUtc - sendTimeUtc;
        }

        /// <summary>
        /// Derives a new unique name from the file name
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public static string GetDerivedTempFileName(string fileName)
        {
            string part1 = fileName.Substring(1, PART1_LENGH);
            string uniqueId = GenerateID();
            return $"t{part1}_{uniqueId}.json";
        }

        /// <summary>
        /// Checks the validity of the queue name
        /// </summary>
        /// <param name="queueName"></param>
        public static void EnsureQueueNameIsValid(string queueName)
        {
            var invalidPathCharactersPresentsInQueueName =
                queueName.ToCharArray()
                    .Intersect(Path.GetInvalidPathChars())
                    .ToList();

            if (!invalidPathCharactersPresentsInQueueName.Any())
                return;

            throw new InvalidOperationException(
                $"Cannot use '{queueName}' as an input queue name because it contains the following invalid characters: {string.Join(", ", invalidPathCharactersPresentsInQueueName.Select(c => $"'{c}'"))}");
        }
    }
}
