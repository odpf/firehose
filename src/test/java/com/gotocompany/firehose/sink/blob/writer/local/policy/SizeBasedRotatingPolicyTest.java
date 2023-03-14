package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SizeBasedRotatingPolicyTest {

    private final SizeBasedRotatingPolicy sizeBasedRotatingPolicy = new SizeBasedRotatingPolicy(256);
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldNeedRotateWhenWriterDataSizeGreaterThanEqualToMaxFileSize() {
        long dataSize = 258L;
        LocalFileMetadata metadata = new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, 100L, dataSize);
        boolean shouldRotate = sizeBasedRotatingPolicy.shouldRotate(metadata);
        Assert.assertTrue(shouldRotate);
    }

    @Test
    public void shouldNotNeedRotateWhenSizeBelowTheLimit() {
        long dataSize = 100L;
        LocalFileMetadata metadata = new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", 1L, 100L, dataSize);
        boolean shouldRotate = sizeBasedRotatingPolicy.shouldRotate(metadata);
        Assert.assertFalse(shouldRotate);
    }

    @Test
    public void shouldThrowExceptionIfInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The max size should be a positive integer");
        new SizeBasedRotatingPolicy(-100);
    }
}
