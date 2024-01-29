public class Main {
    private static final long FIRST_INDEX = 1;
    private static final long GENERATION_FREQUENCY = 5;
    private static final long MESSAGE_MIN_TIME = 100;
    private static final long MESSAGE_MAX_TIME = 1000;
    private static final short NUMBER_OF_THREADS = 8;

    public static void main(String[] args) {
        MessageGenerator messageGenerator = new MessageGenerator(GENERATION_FREQUENCY, FIRST_INDEX, MESSAGE_MIN_TIME, MESSAGE_MAX_TIME);
        Thread messageGeneratorThread = new Thread(messageGenerator);
        messageGeneratorThread.start();

        // Depending on the processing strategy, we can use MessageProcessorWithoutMap which is slower
        // but more memory efficient, or we can use MessageProcessorWithMap which is faster but
        // less memory efficient

        MessageProcessorWithoutMap messageProcessor = new MessageProcessorWithoutMap(messageGenerator.getMessageQueue(), FIRST_INDEX, NUMBER_OF_THREADS);
        messageProcessor.process();

        MessageConsumer messageConsumer = new MessageConsumer(messageProcessor.getOutputMessageQueue(), FIRST_INDEX);
        Thread messageConsumerThread = new Thread(messageConsumer);
        messageConsumerThread.start();
    }
}