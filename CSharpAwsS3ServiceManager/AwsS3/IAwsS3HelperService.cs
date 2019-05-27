using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CSharpAwsS3ServiceManager.AwsS3
{
    public interface IAwsS3HelperService
    {
        Task<bool> UploadFileAsync(Stream fileStream, string fileName, string directory);
        Task<bool> UploadFileAsync(string filePath, string directory);
        Task<bool> UploadFileAsync(string contents, string contentType, string fileName, string directory);
        Task<(Stream FileStream, string ContentType)> ReadFileAsync(string fileName, string directory);
        Task<List<(Stream FileStream, string fileName, string ContentType)>> ReadDirectoryAsync(string directory);
        Task<bool> MoveFileAsync(string fileName, string sourceDirectory, string destDirectory);
        Task<bool> RemoveFileAsync(string fileName, string directory);
    }
}