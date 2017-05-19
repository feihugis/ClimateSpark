package gov.nasa.gsfc.cisto.cds.sia.core.common;

import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaFilePathCompositeKey;
import gov.nasa.gsfc.cisto.cds.sia.core.preprocessing.datasetparsers.SiaVariableCompositeKey;
import org.hibernate.Session;

import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;

/**
 * The type Dao.
 *
 * @param <T> the type parameter
 */
public class DAOImpl<T> implements DAO {

    /**
     * The Session.
     */
    Session session;

    public void setSession(Session session) {
        this.session = session;
    }

    /**
     * Gets session.
     *
     * @return the session
     */
    protected Session getSession() {
        if(!isSetup()) {
            System.exit(-1);
        }

        return session;
    }

    public Object findByName(Class type, String name) {
        if(!isSetup()) {
            System.exit(-1);
        }

        Object result = null;

        try {
            result = session.get(type, name);
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve data via name: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
        return result;
    }

    public Object findVariableByKey(Class type, SiaVariableCompositeKey siaVariableCompositeKey) {
        if(!isSetup()) {
            System.exit(-1);
        }

        Object result = null;

        try {
            result = session.get(type, siaVariableCompositeKey);
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve variable via composite key: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
        return result;
    }

    public Object findFilePathByKey(Class type, SiaFilePathCompositeKey siaFilePathCompositeKey) {
        if(!isSetup()) {
            System.exit(-1);
        }

        Object result = null;

        try {
            result = session.get(type, siaFilePathCompositeKey);
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve file path via composite key: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
        return result;
    }

    public Object findById(String tableName, Integer id) {
        if(!isSetup()) {
            System.exit(-1);
        }

        Object result = null;

        try {
            result = session.get(tableName, id);
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve data via ID: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
        return result;
    }

    public List<T> findAllByType(Class type) {
        if(!isSetup()) {
            System.exit(-1);
        }

        List<T> objects = null;
        CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(type);

        try {
            session.beginTransaction();
            Root<T> siaMetadataRoot = criteriaQuery.from(type);
            criteriaQuery.select(siaMetadataRoot);
            objects = session.createQuery(criteriaQuery).getResultList();
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve data via type: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }

        return objects;
    }

    /** Valid hibernate criteria builder queries
     * .equal      - test equality of two expressions
     * .notEqual   - test inequality of two expressions
     * .gt         - test first expression greater than second
     * .ge         - test first expression greater than or equal to second
     * .lt         - test first expression less than second
     * .le         - test first expression less than or equal to second
     * .between    - test first expression between second and third
     * .like       - test expression matches a given pattern
     *
     * Predicates for criteria builder
     * .and        - logical conjunction of two boolean expressions (see above list)
     * .or         - logical disjunction of two boolean expressions (see above list)
     * .not        - logical negation of a boolean expression (see above list)
     *
     * Example
     * criteriaQuery.where(criteriaBuilder.and(criteriaBuilder.gt(balance, 100),
     *                                         criteriaBuilder.lt(balance, 200)));
     */
    public List<T> findByQuery(String hqlQuery) {
        if(!isSetup()) {
            System.exit(-1);
        }

        List<T> objects = null;

        try {
            session.beginTransaction();
            Query query = session.createQuery(hqlQuery);
            objects = query.getResultList();
            session.close();
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve data via query: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }

        return objects;
    }

    public void insertList(List list) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            for(Object object: list) {
                session.save(object);
            }
            session.getTransaction().commit();
        } catch(Exception e) {
            System.err.println("Unable to insert list into database: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
    }

    public void insert(Object object) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            session.save(object);
            session.getTransaction().commit();
        } catch(Exception e) {
            System.err.println("Unable to insert into database: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
    }

    public void insertDynamicTableObject(String tableName, Object object) {
        if(!isSetup()) {
            System.exit(-1);
        }
        try {
            session.beginTransaction();
            session.save(tableName, object);
            session.getTransaction().commit();
        } catch(Exception e) {
            session.getTransaction().rollback();
        }
    }

    /**
     * Insert dynamic table object list.
     *
     * @param tableName  the table name
     * @param objectList the object list
     */
    public void insertDynamicTableObjectList(String tableName, List<Object> objectList) {
      if(!isSetup()) {
        System.exit(-1);
      }
      try {
        session.beginTransaction();
        for (Object object : objectList) {
          session.save(tableName, object);
        }
        session.getTransaction().commit();
      } catch(Exception e) {
        session.getTransaction().rollback();
      }
    }



    public void update(Object object) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            session.update(object);
        } catch(Exception e) {
            session.getTransaction().rollback();
        }
    }

    public void deleteByName(String name) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            session.delete(name);
        } catch(Exception e) {
            session.getTransaction().rollback();
        }
    }

    private boolean isSetup() {
        if(this.session == null) {
            System.err.println("Application exiting - session factory not properly setup in DAO.");
            return false;
        }
        return true;
    }
}