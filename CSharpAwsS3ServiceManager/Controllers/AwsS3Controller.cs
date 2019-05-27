using System.IO;
using System.Net;
using System.Threading.Tasks;
using CSharpAwsS3ServiceManager.AwsS3;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Net.Http.Headers;

namespace CSharpAwsS3ServiceManager.Controllers
{
    /// <summary>
    /// Test controller for file management in AWS S3 bucket
    /// </summary>
    [Route("api/[controller]")]
    [ApiController]
    public class AwsS3Controller : ControllerBase
    {
        private readonly IAwsS3HelperService _awsS3Service;

        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="awsS3Service"></param>
        public AwsS3Controller(IAwsS3HelperService awsS3Service)
        {
            _awsS3Service = awsS3Service;
        }
        /// <summary>
        /// upload file to s3 bucket
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        [HttpPost]
        public async Task<IActionResult> Upload(IFormFile file)
        {
            if (file.Length == 0)
            {
                return BadRequest("please provide valid file");
            }
            var fileName = ContentDispositionHeaderValue
                .Parse(file.ContentDisposition)
                .FileName
                .TrimStart().ToString();
            var folderName = Request.Form.ContainsKey("folder") ? Request.Form["folder"].ToString() : null;
            bool status;
            using (var fileStream = file.OpenReadStream())
            using (var ms = new MemoryStream())
            {
                await fileStream.CopyToAsync(ms);
                status = await _awsS3Service.UploadFileAsync(ms, fileName, folderName);
            }
            return status ? Ok("success")
                          : StatusCode((int)HttpStatusCode.InternalServerError, $"error uploading {fileName}");
        }
        /// <summary>
        /// endpoint for retrieving content from s3 bucket title directory
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="folder"></param>
        /// <returns></returns>
        [ProducesResponseType((int)HttpStatusCode.BadRequest)]
        [ProducesResponseType((int)HttpStatusCode.NotFound)]
        [ProducesResponseType((int)HttpStatusCode.OK)]
        public async Task<IActionResult> Get(string fileName, string folder)
        {
            if (string.IsNullOrEmpty(fileName) || string.IsNullOrEmpty(folder))
            {
                return BadRequest("please provide valid file or valid folder name");
            }
            var response = await _awsS3Service.ReadFileAsync(fileName, folder);
            if (response.FileStream == null)
            {
                return NotFound();
            }
            return File(response.FileStream, response.ContentType);
        }
        /// <summary>
        /// endpoint for removing files from s3 bucket
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="folder"></param>
        /// <returns></returns>
        [HttpDelete]
        [ProducesResponseType((int)HttpStatusCode.BadRequest)]
        [ProducesResponseType((int)HttpStatusCode.NoContent)]
        [ProducesResponseType((int)HttpStatusCode.InternalServerError)]
        public async Task<IActionResult> Remove(string fileName, string folder)
        {
            if (string.IsNullOrEmpty(fileName) || string.IsNullOrEmpty(folder))
            {
                return BadRequest("please provide valid file and/or valid folder name");
            }
            ;
            if (await _awsS3Service.RemoveFileAsync(fileName, folder))
            {
                return NoContent();
            }
            return StatusCode((int)HttpStatusCode.InternalServerError, $"error removing {fileName}");
        }

    }
}