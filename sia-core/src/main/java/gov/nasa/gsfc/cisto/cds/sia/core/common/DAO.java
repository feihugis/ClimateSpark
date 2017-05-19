package gov.nasa.gsfc.cisto.cds.sia.core.common;

import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaFilePathCompositeKey;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaVariableCompositeKey;
import org.hibernate.Session;

import java.util.List;

/**
 * The interface Dao.
 *
 * @param <T> the type parameter
 */
public interface DAO<T> {

    /**
     * Sets session.
     *
     * @param session the session
     */
    void setSession(Session session);

    /**
     * Find by name t.
     *
     * @param type the type
     * @param name the name
     * @return the t
     */
// Read
    T findByName(Class<T> type, String name);

    /**
     * Find variable by key t.
     *
     * @param type                    the type
     * @param siaVariableCompositeKey the sia variable composite key
     * @return the t
     */
    T findVariableByKey(Class<T> type, SiaVariableCompositeKey siaVariableCompositeKey);

    /**
     * Find file path by key t.
     *
     * @param type                    the type
     * @param siaFilePathCompositeKey the sia file path composite key
     * @return the t
     */
    T findFilePathByKey(Class<T> type, SiaFilePathCompositeKey siaFilePathCompositeKey);

    /**
     * Find by id t.
     *
     * @param tableName the table name
     * @param id        the id
     * @return the t
     */
    T findById(String tableName, Integer id);

    /**
     * Find all by type list.
     *
     * @param type the type
     * @return the list
     */
    List<T> findAllByType(Class<T> type);

    /**
     * Find by query list.
     *
     * @param hqlQuery the hql query
     * @return the list
     */
    List<T> findByQuery(String hqlQuery);

    /**
     * Insert list.
     *
     * @param list the list
     */
// Create
    void insertList(List<T> list);

    /**
     * Insert.
     *
     * @param object the object
     */
    void insert(T object);

    /**
     * Insert dynamic table object.
     *
     * @param tableName the table name
     * @param object    the object
     */
    void insertDynamicTableObject(String tableName, Object object);

    /**
     * Update.
     *
     * @param object the object
     */
// Update
    void update(T object);

    /**
     * Delete by name.
     *
     * @param name the name
     */
// Delete
    void deleteByName(String name);
}