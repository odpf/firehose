package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimeBasedRotatingPolicyTest {

    private final TimeBasedRotatingPolicy rotatingPolicy = new TimeBasedRotatingPolicy(200);
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldRotateWhenElapsedTimeGreaterThanMaxRotatingDuration() throws InterruptedException {
        long createdTimestamp = System.currentTimeMillis();
        LocalFileMetadata metadata = new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", createdTimestamp, 100L, 100L);
        Thread.sleep(300);
        boolean shouldRotate = rotatingPolicy.shouldRotate(metadata);
        Assert.assertTrue(shouldRotate);
    }

    @Test
    public void shouldNotRotateWhenElapsedTimeLessThanMaxRotatingDuration() throws InterruptedException {
        long createdTimestamp = System.currentTimeMillis();
        LocalFileMetadata metadata = new LocalFileMetadata("/tmp", "/tmp/a/random-file-name-1", createdTimestamp, 100L, 100L);
        Thread.sleep(100);
        boolean shouldRotate = rotatingPolicy.shouldRotate(metadata);
        Assert.assertFalse(shouldRotate);
    }

    @Test
    public void shouldThrowExceptionIfInvalid() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("The max duration should be a positive integer");
        new TimeBasedRotatingPolicy(-100);
    }
}
