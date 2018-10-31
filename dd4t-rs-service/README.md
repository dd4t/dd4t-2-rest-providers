Note: if you use JMS invalidation with ActiveMQ, you need to whitelist packages which can be serialized. This is done by adding the following Java startup parameter to wherever you are using this:

	-Dorg.apache.activemq.SERIALIZABLE_PACKAGES=com.tridion.cache,org.apache.activemq,com.thoughtworks.xstream