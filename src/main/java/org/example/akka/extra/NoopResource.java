package org.example.akka.extra;

import net.enilink.komma.core.IEntityManager;
import net.enilink.komma.core.IReference;
import net.enilink.komma.core.URI;
import org.example.akka.extra.IResource;

/**
 * No-operation implementation of IResource.
 * Provides safe defaults without requiring mocks or external setup.
 */
public class NoopResource implements IResource {
    private final URI uri;
    private final IReference ref;

    public NoopResource() {
        this.uri = null;
        this.ref = null;
    }

    public NoopResource(URI uri, IReference ref) {
        this.uri = uri;
        this.ref = ref;
    }

    @Override
    public Object getSingle(IReference var1) {
        return ref;
    }

    @Override
    public <T> T as(Class<T> aClass) {
        return null;
    }

    @Override
    public IEntityManager getEntityManager() {
        return null;
    }

    @Override
    public void refresh() {
        // no-op
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public IReference getReference() {
        return ref;
    }
}
