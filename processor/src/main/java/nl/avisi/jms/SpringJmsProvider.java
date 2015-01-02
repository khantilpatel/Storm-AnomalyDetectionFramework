package nl.avisi.jms;

import org.springframework.context.ApplicationContext;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import backtype.storm.contrib.jms.JmsProvider;

public class SpringJmsProvider implements JmsProvider {
    private ConnectionFactory connectionFactory;
    private Destination destination;

    /**
     * Constructs a <code>SpringJmsProvider</code> object given the name of a
     * classpath resource (the spring application context file), and the bean
     * names of a JMS connection factory and destination.
     *
     * @param context - the spring configuration file (classpath resource)
     * @param connectionFactoryBean - the JMS connection factory bean name
     * @param destinationBean - the JMS destination bean name
     */
    public SpringJmsProvider(ApplicationContext context, String connectionFactoryBean, String destinationBean){
        this.connectionFactory = (ConnectionFactory)context.getBean(connectionFactoryBean);
        this.destination = (Destination)context.getBean(destinationBean);
    }

    public ConnectionFactory connectionFactory() throws Exception {
        return this.connectionFactory;
    }

    public Destination destination() throws Exception {
        return this.destination;
    }

}