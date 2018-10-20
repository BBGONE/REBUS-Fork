using System;
using System.IO;
using System.Linq;

namespace Rebus.Transport.FileSystem
{
    class FileSystemHelper
    {
        public static readonly DateTime historicalDate = new DateTime(1970, 1, 1, 0, 0, 0);

        public static bool RenameFile(string fullPath, string newFileName, out string newFilePath)
        {
            newFilePath = string.Empty;
            try
            {
                string dirName = Path.GetDirectoryName(fullPath);
                string fileName = Path.GetFileName(fullPath);
                newFilePath = Path.Combine(dirName, newFileName);
                FileInfo fileInfo = new FileInfo(fullPath);
                fileInfo.MoveTo(newFilePath);
                return true;
            }
            catch (IOException)
            {
                // the file is gone
                newFilePath = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// change the first letter from any to t (means temporary)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <returns></returns>
        public static bool RenameToTemp(string fullPath, out string newFilePath)
        {
            string fileName = Path.GetFileName(fullPath);
            string newFileName = $"t{fileName.Substring(1)}";
            return RenameFile(fullPath, newFileName, out newFilePath);
        }

        /// <summary>
        /// change the first letter from any to e (means error)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <returns></returns>
        public static bool RenameToError(string fullPath, out string newFilePath)
        {
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
