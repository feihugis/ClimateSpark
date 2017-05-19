package gov.nasa.gsfc.cisto.cds.sia.core;

import gov.nasa.gsfc.cisto.cds.sia.core.collection.SiaCollection;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;


/**
 * The type Hibernate utils.
 */
public class HibernateUtils {

    private static StandardServiceRegistry registry;
    private static SessionFactory sessionFactory;

    /**
     * Create session factory with physical naming strategy session factory.
     *
     * @param hibernateConfigXml the hibernate config xml
     * @param siaCollection      the sia collection
     * @return the session factory
     */
    public static SessionFactory createSessionFactoryWithPhysicalNamingStrategy(String hibernateConfigXml,
                                                                                SiaCollection siaCollection) {
        registry = new StandardServiceRegistryBuilder().configure(hibernateConfigXml).build();

        try {
            MetadataSources sources = new MetadataSources(registry);
            MetadataBuilder metadataBuilder = sources.getMetadataBuilder();

            metadataBuilder.applyPhysicalNamingStrategy(siaCollection);
            sessionFactory = metadataBuilder.build().buildSessionFactory();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Hibernate session factory setup error: " + e);
            StandardServiceRegistryBuilder.destroy(registry);
        }

        return sessionFactory;
    }

    /**
     * Create session factory session factory.
     *
     * @param hibernateConfigXml the hibernate config xml
     * @return the session factory
     */
    public static SessionFactory createSessionFactory(String hibernateConfigXml) {
        registry = new StandardServiceRegistryBuilder().configure(hibernateConfigXml).build();

        try {
            sessionFactory = new MetadataSources(registry).buildMetadata().buildSessionFactory();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Hibernate session factory setup error: " + e);
            StandardServiceRegistryBuilder.destroy(registry);
        }

        return sessionFactory;
    }

    //public static EntityManager createEntityManagerFactory()
}
