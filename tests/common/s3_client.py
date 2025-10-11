"""
S3 Client wrapper for test operations
"""

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


# Default capabilities when none are provided
DEFAULT_CAPABILITIES = {
    "sigv4_chunked": True,
    "unsigned_payload_allowed": True,
    "virtual_hosted_default": True,
    "list_objects_v1": False,
    "list_objects_url_plus_treated_as_space": False,
    "retry_mode": "standard",
    "follows_301_region_redirect": True,
    "follows_307_on_put": True,
    "crc32c_default": False,
}


class S3Client:
    """
    Wrapper around boto3 S3 client with vendor-neutral operations
    """

    def __init__(
        self,
        endpoint_url: str = None,
        access_key: str = None,
        secret_key: str = None,
        region: str = "us-east-1",
        use_ssl: bool = True,
        verify_ssl: bool = True,
        capabilities: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize S3 client

        Args:
            endpoint_url: S3 endpoint URL
            access_key: Access key ID
            secret_key: Secret access key
            region: AWS region
            use_ssl: Use SSL/TLS
            verify_ssl: Verify SSL certificates
            capabilities: SDK capability profile (optional)
        """
        self.endpoint_url = endpoint_url
        self.region = region
        self.capabilities = capabilities or DEFAULT_CAPABILITIES.copy()

        # Create boto3 client
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            use_ssl=use_ssl,
            verify=verify_ssl,
        )

        # Create boto3 resource for higher-level operations
        self.resource = boto3.resource(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            use_ssl=use_ssl,
            verify=verify_ssl,
        )

    def get_capability(self, key: str, default: Any = None) -> Any:
        """Get a specific capability value"""
        return self.capabilities.get(key, default)

    def has_capability(self, key: str) -> bool:
        """Check if a capability is enabled (truthy)"""
        return bool(self.capabilities.get(key, False))

    # Bucket operations
    def create_bucket(self, bucket_name: str, **kwargs) -> Dict[str, Any]:
        """Create a bucket"""
        try:
            # Handle region-specific bucket creation
            if self.region and self.region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.region
                }

            response = self.client.create_bucket(Bucket=bucket_name, **kwargs)
            logger.debug(f"Created bucket: {bucket_name}")
            return response
        except ClientError as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            raise

    def delete_bucket(self, bucket_name: str) -> Dict[str, Any]:
        """Delete a bucket"""
        try:
            response = self.client.delete_bucket(Bucket=bucket_name)
            logger.debug(f"Deleted bucket: {bucket_name}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting bucket {bucket_name}: {e}")
            raise

    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all buckets"""
        try:
            response = self.client.list_buckets()
            return response.get("Buckets", [])
        except ClientError as e:
            logger.error(f"Error listing buckets: {e}")
            raise

    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists"""
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def get_bucket_location(self, bucket_name: str) -> str:
        """Get bucket location/region"""
        try:
            response = self.client.get_bucket_location(Bucket=bucket_name)
            return response.get("LocationConstraint", "us-east-1")
        except ClientError as e:
            logger.error(f"Error getting bucket location: {e}")
            raise

    # Object operations
    def put_object(
        self, bucket_name: str, key: str, body: bytes = b"", **kwargs
    ) -> Dict[str, Any]:
        """Put an object"""
        try:
            response = self.client.put_object(
                Bucket=bucket_name, Key=key, Body=body, **kwargs
            )
            logger.debug(f"Put object: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error putting object {bucket_name}/{key}: {e}")
            raise

    def get_object(self, bucket_name: str, key: str, **kwargs) -> Dict[str, Any]:
        """Get an object"""
        try:
            response = self.client.get_object(Bucket=bucket_name, Key=key, **kwargs)
            logger.debug(f"Got object: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error getting object {bucket_name}/{key}: {e}")
            raise

    def head_object(self, bucket_name: str, key: str, **kwargs) -> Dict[str, Any]:
        """Get object metadata without downloading the object"""
        try:
            response = self.client.head_object(Bucket=bucket_name, Key=key, **kwargs)
            logger.debug(f"Head object: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error getting head for object {bucket_name}/{key}: {e}")
            raise

    def delete_object(self, bucket_name: str, key: str, **kwargs) -> Dict[str, Any]:
        """Delete an object"""
        try:
            response = self.client.delete_object(Bucket=bucket_name, Key=key, **kwargs)
            logger.debug(f"Deleted object: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error deleting object {bucket_name}/{key}: {e}")
            raise

    def list_objects(
        self, bucket_name: str, prefix: str = "", max_keys: int = 1000, **kwargs
    ) -> List[Dict[str, Any]]:
        """List objects in a bucket"""
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys, **kwargs
            )
            return response.get("Contents", [])
        except ClientError as e:
            logger.error(f"Error listing objects in {bucket_name}: {e}")
            raise

    def object_exists(self, bucket_name: str, key: str) -> bool:
        """Check if an object exists"""
        try:
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
        **kwargs,
    ) -> Dict[str, Any]:
        """Copy an object"""
        try:
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            response = self.client.copy_object(
                CopySource=copy_source, Bucket=dest_bucket, Key=dest_key, **kwargs
            )
            logger.debug(
                f"Copied object: {source_bucket}/{source_key} -> {dest_bucket}/{dest_key}"
            )
            return response
        except ClientError as e:
            logger.error(f"Error copying object: {e}")
            raise

    # Multipart upload operations
    def create_multipart_upload(self, bucket_name: str, key: str, **kwargs) -> str:
        """Initiate a multipart upload"""
        try:
            response = self.client.create_multipart_upload(
                Bucket=bucket_name, Key=key, **kwargs
            )
            logger.debug(f"Created multipart upload: {bucket_name}/{key}")
            return response["UploadId"]
        except ClientError as e:
            logger.error(f"Error creating multipart upload: {e}")
            raise

    def upload_part(
        self,
        bucket_name: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
        **kwargs,
    ) -> Dict[str, Any]:
        """Upload a part in multipart upload"""
        try:
            response = self.client.upload_part(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=body,
                **kwargs,
            )
            logger.debug(f"Uploaded part {part_number} for {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error uploading part: {e}")
            raise

    def complete_multipart_upload(
        self, bucket_name: str, key: str, upload_id: str, parts: List[Dict], **kwargs
    ) -> Dict[str, Any]:
        """Complete a multipart upload"""
        try:
            response = self.client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
                **kwargs,
            )
            logger.debug(f"Completed multipart upload: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error completing multipart upload: {e}")
            raise

    def abort_multipart_upload(
        self, bucket_name: str, key: str, upload_id: str
    ) -> Dict[str, Any]:
        """Abort a multipart upload"""
        try:
            response = self.client.abort_multipart_upload(
                Bucket=bucket_name, Key=key, UploadId=upload_id
            )
            logger.debug(f"Aborted multipart upload: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error aborting multipart upload: {e}")
            raise

    # ACL operations
    def put_bucket_acl(
        self, bucket_name: str, acl: str = None, **kwargs
    ) -> Dict[str, Any]:
        """Set bucket ACL"""
        try:
            if acl:
                kwargs["ACL"] = acl
            response = self.client.put_bucket_acl(Bucket=bucket_name, **kwargs)
            logger.debug(f"Set bucket ACL: {bucket_name}")
            return response
        except ClientError as e:
            logger.error(f"Error setting bucket ACL: {e}")
            raise

    def get_bucket_acl(self, bucket_name: str) -> Dict[str, Any]:
        """Get bucket ACL"""
        try:
            response = self.client.get_bucket_acl(Bucket=bucket_name)
            logger.debug(f"Got bucket ACL: {bucket_name}")
            return response
        except ClientError as e:
            logger.error(f"Error getting bucket ACL: {e}")
            raise

    def put_object_acl(
        self, bucket_name: str, key: str, acl: str = None, **kwargs
    ) -> Dict[str, Any]:
        """Set object ACL"""
        try:
            if acl:
                kwargs["ACL"] = acl
            response = self.client.put_object_acl(Bucket=bucket_name, Key=key, **kwargs)
            logger.debug(f"Set object ACL: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error setting object ACL: {e}")
            raise

    def get_object_acl(self, bucket_name: str, key: str) -> Dict[str, Any]:
        """Get object ACL"""
        try:
            response = self.client.get_object_acl(Bucket=bucket_name, Key=key)
            logger.debug(f"Got object ACL: {bucket_name}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Error getting object ACL: {e}")
            raise

    # Versioning operations
    def put_bucket_versioning(
        self, bucket_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enable/disable bucket versioning"""
        try:
            response = self.client.put_bucket_versioning(
                Bucket=bucket_name, VersioningConfiguration=config
            )
            logger.debug(f"Set bucket versioning: {bucket_name} -> {config}")
            return response
        except ClientError as e:
            logger.error(f"Error setting bucket versioning: {e}")
            raise

    def get_bucket_versioning(self, bucket_name: str) -> Dict[str, Any]:
        """Get bucket versioning status"""
        try:
            response = self.client.get_bucket_versioning(Bucket=bucket_name)
            return response
        except ClientError as e:
            logger.error(f"Error getting bucket versioning: {e}")
            raise

    def list_object_versions(self, bucket_name: str, **kwargs) -> Dict[str, Any]:
        """List object versions in a bucket"""
        try:
            response = self.client.list_object_versions(Bucket=bucket_name, **kwargs)
            return response
        except ClientError as e:
            logger.error(f"Error listing object versions: {e}")
            raise

    # Lifecycle operations
    def put_bucket_lifecycle(
        self, bucket_name: str, rules: List[Dict]
    ) -> Dict[str, Any]:
        """Set bucket lifecycle rules"""
        try:
            response = self.client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration={"Rules": rules}
            )
            logger.debug(f"Set bucket lifecycle: {bucket_name}")
            return response
        except ClientError as e:
            logger.error(f"Error setting bucket lifecycle: {e}")
            raise

    def get_bucket_lifecycle(self, bucket_name: str) -> List[Dict]:
        """Get bucket lifecycle rules"""
        try:
            response = self.client.get_bucket_lifecycle_configuration(
                Bucket=bucket_name
            )
            return response.get("Rules", [])
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                return []
            logger.error(f"Error getting bucket lifecycle: {e}")
            raise

    # Encryption operations
    def put_bucket_encryption(
        self, bucket_name: str, sse_algorithm: str = "AES256", kms_key_id: str = None
    ) -> Dict[str, Any]:
        """Set bucket encryption"""
        try:
            rules = [
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": sse_algorithm}}
            ]
            if kms_key_id and sse_algorithm == "aws:kms":
                rules[0]["ApplyServerSideEncryptionByDefault"][
                    "KMSMasterKeyID"
                ] = kms_key_id

            response = self.client.put_bucket_encryption(
                Bucket=bucket_name, ServerSideEncryptionConfiguration={"Rules": rules}
            )
            logger.debug(f"Set bucket encryption: {bucket_name}")
            return response
        except ClientError as e:
            logger.error(f"Error setting bucket encryption: {e}")
            raise

    def get_bucket_encryption(self, bucket_name: str) -> Dict[str, Any]:
        """Get bucket encryption configuration"""
        try:
            response = self.client.get_bucket_encryption(Bucket=bucket_name)
            return response.get("ServerSideEncryptionConfiguration", {})
        except ClientError as e:
            if (
                e.response["Error"]["Code"]
                == "ServerSideEncryptionConfigurationNotFoundError"
            ):
                return {}
            logger.error(f"Error getting bucket encryption: {e}")
            raise

    # Utility methods
    def generate_presigned_url(
        self,
        bucket_name: str,
        key: str,
        operation: str = "get_object",
        expires_in: int = 3600,
    ) -> str:
        """Generate a presigned URL"""
        try:
            url = self.client.generate_presigned_url(
                ClientMethod=operation,
                Params={"Bucket": bucket_name, "Key": key},
                ExpiresIn=expires_in,
            )
            logger.debug(f"Generated presigned URL for {bucket_name}/{key}")
            return url
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {e}")
            raise

    def upload_file(
        self,
        filename: str,
        bucket_name: str,
        key: str,
        transfer_config: TransferConfig = None,
    ) -> None:
        """Upload a file using managed transfer"""
        try:
            self.client.upload_file(
                Filename=filename, Bucket=bucket_name, Key=key, Config=transfer_config
            )
            logger.debug(f"Uploaded file: {filename} -> {bucket_name}/{key}")
        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            raise

    def download_file(
        self,
        bucket_name: str,
        key: str,
        filename: str,
        transfer_config: TransferConfig = None,
    ) -> None:
        """Download a file using managed transfer"""
        try:
            self.client.download_file(
                Bucket=bucket_name, Key=key, Filename=filename, Config=transfer_config
            )
            logger.debug(f"Downloaded file: {bucket_name}/{key} -> {filename}")
        except ClientError as e:
            logger.error(f"Error downloading file: {e}")
            raise

    def empty_bucket(self, bucket_name: str) -> int:
        """Delete all objects in a bucket"""
        try:
            count = 0
            bucket = self.resource.Bucket(bucket_name)
            for obj in bucket.objects.all():
                obj.delete()
                count += 1
            logger.debug(f"Deleted {count} objects from {bucket_name}")
            return count
        except ClientError as e:
            logger.error(f"Error emptying bucket: {e}")
            raise
