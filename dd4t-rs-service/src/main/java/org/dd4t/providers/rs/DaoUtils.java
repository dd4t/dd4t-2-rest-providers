package org.dd4t.providers.rs;

import com.tridion.broker.StorageException;
import com.tridion.storage.StorageManagerFactory;
import com.tridion.storage.StorageTypeMapping;
import com.tridion.storage.dao.BaseDAO;
import com.tridion.storage.dao.WrappableDAO;
import com.tridion.storage.persistence.JPABaseDAO;

/**
 * dd4t-remote-providers
 *
 * @author R. Kempees
 */
public class DaoUtils {
	public static JPABaseDAO getJPADAO (int publicationId, StorageTypeMapping storageTypeMapping) throws StorageException {
		BaseDAO baseDAO = StorageManagerFactory.getDAO(publicationId, storageTypeMapping);
		boolean loop = true;

		while (loop) {
			if (baseDAO instanceof WrappableDAO) {
				WrappableDAO wrappableDAO = (WrappableDAO) baseDAO;
				baseDAO = wrappableDAO.getWrapped();
			} else {
				loop = false;
			}

			if (baseDAO instanceof JPABaseDAO) {
				return (JPABaseDAO) baseDAO;
			}
		}
		throw new StorageException("Cannot find JPABaseDAO. Please check your storage bindings.");
	}

	private DaoUtils (){

	}
}
