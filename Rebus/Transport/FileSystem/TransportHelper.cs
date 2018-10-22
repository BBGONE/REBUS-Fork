using Rebus.Time;
using System;
using System.IO;
using System.Linq;
using System.Threading;

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
        /// change the first letter from any to t (means temporary)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <param name="newFilePath"></param>
        /// <returns></returns>
        public static bool RenameToTempWithLock(string fullPath, out string newFilePath)
        {
            string dirName = Path.GetDirectoryName(fullPath);
            newFilePath = null;
            string fileName = Path.GetFileName(fullPath);
            string lockFileName = $"l{fileName.Substring(1)}";
            string lockFilePath = Path.Combine(dirName, lockFileName);
            try
            {
                using (var fileLock = new FileStream(lockFilePath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None, 4096, FileOptions.DeleteOnClose))
                {
                    string newFileName = $"t{fileName.Substring(1)}";
                    return RenameFile(fullPath, newFileName, out newFilePath);
                }
            }
            catch (Exception)
            {
                return false;
            }
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
        /// <param name="fileName"></param>
        /// <returns></returns>
        public static DateTime GetFileUtcDate(string fileName)
        {
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
        /// Get the DateTime ticks in the form of the padded with '0' string
        /// </summary>
        /// <returns></returns>
        public static string GetTimeTicks(DateTime date)
        {
            return (date.Ticks - TransportHelper.historicalDate.Ticks).ToString().PadLeft(19, '0');
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
