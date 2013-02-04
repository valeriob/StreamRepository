//-----------------------------------------------------------------------
// <copyright file="AzureBlobStorageExtensions.cs" company="Andrew Arnott">
//     Copyright (c) Andrew Arnott. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public static class AzureBlobStorageExtensions
    {
        public static Task<bool> CreateIfNotExistAsync(this CloudBlobContainer container, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Factory.FromAsync(
                (cb, state) => (IAsyncResult)((CloudBlobContainer)state).BeginCreateIfNotExists(cb, state).WithCancellation(cancellationToken),
                ar => ((CloudBlobContainer)ar.AsyncState).EndCreateIfNotExists(ar),
                container);
        }

        public static async Task<ReadOnlyCollection<IListBlobItem>> ListBlobsSegmentedAsync(
            this CloudBlobContainer container,
            string prefix,
            bool useFlatBlobListing,
            int pageSize,
            BlobListingDetails details,
            BlobRequestOptions options,
            OperationContext operationContext,
            IProgress<IEnumerable<IListBlobItem>> progress = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            options = options ?? new BlobRequestOptions();
            var results = new List<IListBlobItem>();
            BlobContinuationToken continuation = null;
            BlobResultSegment segment;
            do
            {
                segment = await Task.Factory.FromAsync(
                    (cb, state) => container.BeginListBlobsSegmented(prefix, useFlatBlobListing, details, pageSize, continuation, options, operationContext, cb, state).WithCancellation(cancellationToken),
                    ar => container.EndListBlobsSegmented(ar),
                    null);
                if (progress != null)
                {
                    progress.Report(segment.Results);
                }
                results.AddRange(segment.Results);
                continuation = segment.ContinuationToken;
            } while (continuation != null);

            return new ReadOnlyCollection<IListBlobItem>(results);
        }

        public static async Task<ReadOnlyCollection<IListBlobItem>> ListBlobsSegmentedAsync(
            this CloudBlobContainer directory,
            IProgress<IEnumerable<IListBlobItem>> progress = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var results = new List<IListBlobItem>();
            BlobContinuationToken continuation = null;
            BlobResultSegment segment;
            do
            {
                segment = await Task.Factory.FromAsync(
                    (cb, state) => directory.BeginListBlobsSegmented(continuation, cb, state).WithCancellation(cancellationToken),
                    ar => directory.EndListBlobsSegmented(ar),
                    null);
                if (progress != null)
                {
                    progress.Report(segment.Results);
                }
                results.AddRange(segment.Results);
                continuation = segment.ContinuationToken;
            } while (continuation != null);

            return new ReadOnlyCollection<IListBlobItem>(results);
        }

        public static async Task<ReadOnlyCollection<IListBlobItem>> ListBlobsSegmentedAsync(
            this CloudBlobDirectory container,
            bool useFlatBlobListing,
            int pageSize,
            BlobListingDetails details,
            BlobRequestOptions options,
            OperationContext operationContext,
            IProgress<IEnumerable<IListBlobItem>> progress = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            options = options ?? new BlobRequestOptions();
            var results = new List<IListBlobItem>();
            BlobContinuationToken continuation = null;
            BlobResultSegment segment;
            do
            {
                segment = await Task.Factory.FromAsync(
                    (cb, state) => container.BeginListBlobsSegmented(useFlatBlobListing, details, pageSize, continuation, options, operationContext, cb, state).WithCancellation(cancellationToken),
                    ar => container.EndListBlobsSegmented(ar),
                    null);
                if (progress != null)
                {
                    progress.Report(segment.Results);
                }
                results.AddRange(segment.Results);
                continuation = segment.ContinuationToken;
            } while (continuation != null);

            return new ReadOnlyCollection<IListBlobItem>(results);
        }

        public static async Task<ReadOnlyCollection<IListBlobItem>> ListBlobsSegmentedAsync(
            this CloudBlobDirectory directory,
            IProgress<IEnumerable<IListBlobItem>> progress = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var results = new List<IListBlobItem>();
            BlobContinuationToken continuation = null;
            BlobResultSegment segment;
            do
            {
                segment = await Task.Factory.FromAsync(
                    (cb, state) => directory.BeginListBlobsSegmented(continuation, cb, state).WithCancellation(cancellationToken),
                    ar => directory.EndListBlobsSegmented(ar),
                    null);
                if (progress != null)
                {
                    progress.Report(segment.Results);
                }
                results.AddRange(segment.Results);
                continuation = segment.ContinuationToken;
            } while (continuation != null);

            return new ReadOnlyCollection<IListBlobItem>(results);
        }

        public static Task DownloadToStreamAsync(this ICloudBlob blob, Stream stream)
        {
            return Task.Factory.FromAsync(
                (cb, state) => ((Tuple<ICloudBlob, Stream>)state).Item1.BeginDownloadToStream(((Tuple<ICloudBlob, Stream>)state).Item2, cb, state),
                ar => ((Tuple<ICloudBlob, Stream>)ar.AsyncState).Item1.EndDownloadToStream(ar),
                Tuple.Create(blob, stream));
        }

        public static Task DownloadRangeToStreamAsync(this ICloudBlob blob, Stream stream, long offset, long length)
        {
            return Task.Factory.FromAsync(
                (cb, state) => ((Tuple<ICloudBlob, Stream>)state).Item1.BeginDownloadRangeToStream(((Tuple<ICloudBlob, Stream>)state).Item2, offset, length, cb, state),
                ar => ((Tuple<ICloudBlob, Stream>)ar.AsyncState).Item1.EndDownloadRangeToStream(ar),
                Tuple.Create(blob, stream));
        }
        
        public static Task UploadFromStreamAsync(this ICloudBlob blob, Stream stream)
        {
            return Task.Factory.FromAsync(
                (cb, state) => ((Tuple<ICloudBlob, Stream>)state).Item1.BeginUploadFromStream(((Tuple<ICloudBlob, Stream>)state).Item2, cb, state),
                ar => ((Tuple<ICloudBlob, Stream>)ar.AsyncState).Item1.EndUploadFromStream(ar),
                Tuple.Create(blob, stream));
        }

        public static Task DeleteAsync(this ICloudBlob blob)
        {
            return Task.Factory.FromAsync(
                (cb, state) => ((ICloudBlob)state).BeginDelete(cb, state),
                ar => ((ICloudBlob)ar.AsyncState).EndDelete(ar),
                blob);
        }

        public static Task<bool> DeleteIfExistsAsync(this ICloudBlob blob)
        {
            return Task.Factory.FromAsync(
                (cb, state) => ((ICloudBlob)state).BeginDeleteIfExists(cb, state),
                ar => ((ICloudBlob)ar.AsyncState).EndDeleteIfExists(ar),
                blob);
        }

        public static Task SetMetadataAsync(this ICloudBlob blob)
        {
            return Task.Factory.FromAsync(
                (cb, state) => ((ICloudBlob)state).BeginSetMetadata(cb, state),
                ar => ((ICloudBlob)ar.AsyncState).EndSetMetadata(ar),
                blob);
        }

        public static Task FetchAttributesAsync(this ICloudBlob blob)
        {
            return Task.Factory.FromAsync(
                (cb, state) => ((ICloudBlob)state).BeginFetchAttributes(cb, state),
                ar => ((ICloudBlob)ar.AsyncState).EndFetchAttributes(ar),
                blob);
        }

        public static Task SetPermissionsAsync(this CloudBlobContainer container, BlobContainerPermissions permissions)
        {
            return Task.Factory.FromAsync(
                (cb, state) => container.BeginSetPermissions(permissions, cb, state),
                ar => container.EndSetPermissions(ar),
                null);
        }

        public static Task WritePagesAsync(this CloudPageBlob blob, Stream pageData, int startOffset)
        {
            return Task.Factory.FromAsync(
                (cb, state) => blob.BeginWritePages(pageData, startOffset, null, cb, state),
                ar => blob.EndWritePages(ar),
                null);
        }
        public static Task ResizeAsync(this CloudPageBlob blob, long size)
        {
            return Task.Factory.FromAsync(
                (cb, state) => blob.BeginResize(size,  null, null, null, cb, state),
                ar => blob.EndResize(ar),
                null);
        }


        public static Task WritePagesAsync(this CloudPageBlob blob, byte[] pageData, int start, int count, int startOffset)
        {
            MemoryStream stream = null;
            return Task.Factory.FromAsync(
                (cb, state) => 
                {
                    stream = new MemoryStream(pageData, start, count);
                    var ar = blob.BeginWritePages(stream, startOffset, null, cb, state);
                    return ar;
                },
                ar => 
                { 
                    blob.EndWritePages(ar); 
                    stream.Dispose();
                },
                null);
        }
    }

    internal static class InternalExtensions
    {
        internal static IAsyncResult WithCancellation(this ICancellableAsyncResult asyncResult, CancellationToken cancellationToken = default(CancellationToken))
        {
            cancellationToken.Register(ar => ((ICancellableAsyncResult)ar).Cancel(), asyncResult, false);
            return asyncResult;
        }
    }
}