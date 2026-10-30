from pathlib import Path
import unittest


TEMPLATE = Path(__file__).resolve().parents[1] / "infrastructure" / "s3-data-lake.yml"


class S3InfrastructureContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = TEMPLATE.read_text(encoding="utf-8")

    def test_bucket_blocks_public_access_and_disables_acls(self):
        self.assertIn("BlockPublicAcls: true", self.template)
        self.assertIn("BlockPublicPolicy: true", self.template)
        self.assertIn("IgnorePublicAcls: true", self.template)
        self.assertIn("RestrictPublicBuckets: true", self.template)
        self.assertIn("ObjectOwnership: BucketOwnerEnforced", self.template)

    def test_bucket_encryption_and_transport_policy_are_present(self):
        self.assertIn("SSEAlgorithm: AES256", self.template)
        self.assertIn("Sid: DenyInsecureTransport", self.template)
        self.assertIn("aws:SecureTransport: 'false'", self.template)

    def test_bucket_is_retained_and_abandoned_uploads_are_cleaned_up(self):
        self.assertIn("DeletionPolicy: Retain", self.template)
        self.assertIn("UpdateReplacePolicy: Retain", self.template)
        self.assertIn("DaysAfterInitiation: 7", self.template)


if __name__ == "__main__":
    unittest.main()
