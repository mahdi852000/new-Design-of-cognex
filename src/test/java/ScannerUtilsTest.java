import lombok.extern.slf4j.Slf4j;
import net.enilink.komma.core.IReference;
import org.example.akka.extra.IResource;

import org.example.akka.extra.LOGISTICS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import org.example.akka.utils.ScannerUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
class ScannerUtilsTest {


    private IResource mockResource;
    private IReference mockRef;

    @BeforeEach
    void setUp() {
        mockResource = mock(IResource.class);
        mockRef = mock(IReference.class);
    }

/**
    *- Verifies that ScannerUtils.getProperty correctly retrieves and converts
    * - a resource property value to a Long when the property exists.
    * - Mocks the resource to return "456" for the "triggerRangeMax" reference,
    * - then asserts the Optional is present and equals 456L.
    * - Logs and console output confirm successful retrieval and conversion.
*/
    @Test
    void testGetProperty_ReturnsLongValue() {
        IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("triggerRangeMax");
        when(mockResource.getSingle(expectedRef)).thenReturn("456");

        Optional<Long> result = ScannerUtils.getProperty(mockResource, Long.class, "triggerRangeMax");

        if (result.isPresent()) {
            System.out.println("Returned value: " + result.get());
        } else {
            System.out.println("No value returned!");
        }

        assertTrue(result.isPresent());
        assertEquals(456L, result.get());

        log.info("getProperty returned: {}", result);

        System.out.println("Everything is fine");

    }


    @Test
    void testGetProperty_ReturnsBooleanValue() {
        // Since `ScannerUtils.getProperty` internally converts the key string ("heartbeat") to an IReference,
        // we must mock the response using that exact IReference, not just the raw key string.
        IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("heartbeat");
        when(mockResource.getSingle(expectedRef)).thenReturn("true");

        Optional<Boolean> result = ScannerUtils.getProperty(mockResource, Boolean.class, "heartbeat");

        if (result.isPresent()) {
            System.out.println("Parsed boolean value: " + result.get());
        } else {
            System.out.println("No value returned for boolean property.");
        }
        assertTrue(result.isPresent());
        assertTrue(result.get());
    }

    /**
     * - Verifies that ScannerUtils.getProperty correctly retrieves and converts
     * - a resource property value to a Boolean when the property exists.
     * - Mocks the resource to return "true" for the "heartbeat" reference,
     * - then asserts the Optional is present and the value is true.
     * - Console output confirms successful parsing of the boolean value.
     */
        @Test
        void testGetProperty_ReturnsEmptyIfNull() {

            IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("triggerRangeMax");
            when(mockResource.getSingle(expectedRef)).thenReturn(null);

            Optional<Long> result = ScannerUtils.getProperty(mockResource, Long.class, "triggerRangeMax");

            System.out.println("Result of getProperty when value is null: " + result);

            assertFalse(result.isPresent());
        }
    /**
     * - Verifies that ScannerUtils.getProperty returns an empty Optional when the property
     * - value cannot be converted to the requested type.
     * - Mocks the resource to return a non-numeric string ("notANumber") for a Long property.
     * - Expects a NumberFormatException to be caught internally, with a warning log message.
     * - Asserts that the resulting Optional is empty, confirming invalid conversion handling.
     */
    @Test
    void testGetProperty_InvalidConversion() {
        IReference expectedRef = LOGISTICS.NAMAESPACE_URI.appendLocalPart("notANumber");
        when(mockResource.getSingle(expectedRef)).thenReturn("notANumber");

        Optional<Long> result = ScannerUtils.getProperty(mockResource, Long.class, "notANumber");

        System.out.println("Result of getProperty when value is not valid: " + result);

        assertFalse(result.isPresent());
    }
}
