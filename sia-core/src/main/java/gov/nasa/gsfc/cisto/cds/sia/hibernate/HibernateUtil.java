package gov.nasa.gsfc.cisto.cds.sia.hibernate;

import org.apache.hadoop.conf.Configuration;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gov.nasa.gsfc.cisto.cds.sia.core.config.ConfigParameterKeywords;

/**
 * Created by Fei Hu on 1/25/18.
 */
public class HibernateUtil {

  private StandardServiceRegistry registry;
  private SessionFactory sessionFactory;
  private ThreadLocal<Session> threadLocal;

  public SessionFactory getSessionFactory(Configuration conf, Class mappingClass) {
    if (sessionFactory == null) {
      try {

        // Create registry builder
        StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

        // Hibernate settings equivalent to hibernate.cfg.xml's properties
        Map<String, String> settings = new HashMap<String, String>();
        settings.put(Environment.DRIVER, conf.get(ConfigParameterKeywords.HIBERNATE_DRIEVER));
        settings.put(Environment.URL, conf.get(ConfigParameterKeywords.HIBERNATE_URL));
        settings.put(Environment.USER, conf.get(ConfigParameterKeywords.HIBERNATE_USER));
        settings.put(Environment.PASS, conf.get(ConfigParameterKeywords.HIBERNATE_PASS));
        settings.put(Environment.DIALECT, conf.get(ConfigParameterKeywords.HIBERNATE_DIALECT));
        settings.put(Environment.HBM2DDL_AUTO, conf.get(ConfigParameterKeywords.HIBERNATE_HBM2DDL_AUTO));

        // Apply settings
        registryBuilder.applySettings(settings);

        // Create registry
        registry = registryBuilder.build();

        // Create MetadataSources
        MetadataSources sources = new MetadataSources(registry);

        sources.addAnnotatedClass(mappingClass);

        // Create Metadata
        Metadata metadata = sources.getMetadataBuilder().build();

        // Create SessionFactory
        sessionFactory = metadata.getSessionFactoryBuilder().build();
        threadLocal = new ThreadLocal<Session>();
      } catch (Exception e) {
        e.printStackTrace();
        if (registry != null) {
          StandardServiceRegistryBuilder.destroy(registry);
        }
      }
    }
    return sessionFactory;
  }

  public <T> void createSessionFactoryWithPhysicalNamingStrategy(Configuration conf,
                                                                 PhysicalNameStrategyImpl physicalNameStrategy,
                                                                 Class<T> mappingClass) {
    // Create registry builder
    StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

    // Hibernate settings equivalent to hibernate.cfg.xml's properties
    Map<String, String> settings = new HashMap<String, String>();
    settings.put(Environment.DRIVER, conf.get(ConfigParameterKeywords.HIBERNATE_DRIEVER));
    settings.put(Environment.URL, conf.get(ConfigParameterKeywords.HIBERNATE_URL));
    settings.put(Environment.USER, conf.get(ConfigParameterKeywords.HIBERNATE_USER));
    settings.put(Environment.PASS, conf.get(ConfigParameterKeywords.HIBERNATE_PASS));
    settings.put(Environment.DIALECT, conf.get(ConfigParameterKeywords.HIBERNATE_DIALECT));
    settings.put(Environment.HBM2DDL_AUTO, conf.get(ConfigParameterKeywords.HIBERNATE_HBM2DDL_AUTO));

    // Apply settings
    registryBuilder.applySettings(settings);

    // Create registry
    registry = registryBuilder.build();

    try {
      MetadataSources sources = new MetadataSources(registry);
      sources.addAnnotatedClass(mappingClass);
      //sources.addClass(mappingClass);
      MetadataBuilder metadataBuilder = sources.getMetadataBuilder();

      metadataBuilder.applyPhysicalNamingStrategy(physicalNameStrategy);
      sessionFactory = metadataBuilder.build().buildSessionFactory();
      threadLocal = new ThreadLocal<Session>();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Hibernate session factory setup error: " + e);
      StandardServiceRegistryBuilder.destroy(registry);
    }
  }

  public <T> void createSessionFactoryWithPhysicalNamingStrategy(Configuration conf,
                                                                 PhysicalNameStrategyImpl physicalNameStrategy,
                                                                 List<Class<T>> mappingClassList) {
    // Create registry builder
    StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

    // Hibernate settings equivalent to hibernate.cfg.xml's properties
    Map<String, String> settings = new HashMap<String, String>();
    settings.put(Environment.DRIVER, conf.get(ConfigParameterKeywords.HIBERNATE_DRIEVER));
    settings.put(Environment.URL, conf.get(ConfigParameterKeywords.HIBERNATE_URL));
    settings.put(Environment.USER, conf.get(ConfigParameterKeywords.HIBERNATE_USER));
    settings.put(Environment.PASS, conf.get(ConfigParameterKeywords.HIBERNATE_PASS));
    settings.put(Environment.DIALECT, conf.get(ConfigParameterKeywords.HIBERNATE_DIALECT));
    settings.put(Environment.HBM2DDL_AUTO, conf.get(ConfigParameterKeywords.HIBERNATE_HBM2DDL_AUTO));

    // Apply settings
    registryBuilder.applySettings(settings);

    // Create registry
    registry = registryBuilder.build();

    try {
      MetadataSources sources = new MetadataSources(registry);
      for (Class<T> mappingClass : mappingClassList) {
        sources.addAnnotatedClass(mappingClass);
      }
      //sources.addClass(mappingClass);
      MetadataBuilder metadataBuilder = sources.getMetadataBuilder();

      metadataBuilder.applyPhysicalNamingStrategy(physicalNameStrategy);
      sessionFactory = metadataBuilder.build().buildSessionFactory();
      threadLocal = new ThreadLocal<Session>();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Hibernate session factory setup error: " + e);
      StandardServiceRegistryBuilder.destroy(registry);
    }
  }

  public void createSessionFactory(Configuration conf, List<Class> mappingClassList) {
    // Create registry builder
    StandardServiceRegistryBuilder registryBuilder = new StandardServiceRegistryBuilder();

    // Hibernate settings equivalent to hibernate.cfg.xml's properties
    Map<String, String> settings = new HashMap<String, String>();
    settings.put(Environment.DRIVER, conf.get(ConfigParameterKeywords.HIBERNATE_DRIEVER));
    settings.put(Environment.URL, conf.get(ConfigParameterKeywords.HIBERNATE_URL));
    settings.put(Environment.USER, conf.get(ConfigParameterKeywords.HIBERNATE_USER));
    settings.put(Environment.PASS, conf.get(ConfigParameterKeywords.HIBERNATE_PASS));
    settings.put(Environment.DIALECT, conf.get(ConfigParameterKeywords.HIBERNATE_DIALECT));
    settings.put(Environment.HBM2DDL_AUTO, conf.get(ConfigParameterKeywords.HIBERNATE_HBM2DDL_AUTO));

    // Apply settings
    registryBuilder.applySettings(settings);

    // Create registry
    registry = registryBuilder.build();

    try {
      MetadataSources sources = new MetadataSources(registry);
      for (Class mappingClass : mappingClassList) {
        sources.addAnnotatedClass(mappingClass);
      }
      MetadataBuilder metadataBuilder = sources.getMetadataBuilder();
      sessionFactory = metadataBuilder.build().buildSessionFactory();
      threadLocal = new ThreadLocal<Session>();

    } catch (Exception e) {
      e.printStackTrace();
      System.err.println("Hibernate session factory setup error: " + e);
      StandardServiceRegistryBuilder.destroy(registry);
    }
  }


  public Session getSession() {
    Session session = threadLocal.get();
    if(session == null){
      session = sessionFactory.openSession();
      threadLocal.set(session);
    }
    return session;
  }

  public void closeSession() {
    Session session = threadLocal.get();
    if(session != null){
      session.close();
      threadLocal.set(null);
    }
  }

  public void closeSessionFactory() {
    sessionFactory.close();
    StandardServiceRegistryBuilder.destroy(registry);
  }

  public void shutdown() {
    if (registry != null) {
      sessionFactory.close();
      StandardServiceRegistryBuilder.destroy(registry);
    }
  }
}
