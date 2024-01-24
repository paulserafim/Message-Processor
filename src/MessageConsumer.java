import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class MessageConsumer implements Runnable{
    private final long firstIndex;
    private Queue<Message> consumerQueue;

    public MessageConsumer(Queue<Message> consumerQueue, long firstIndex) {
        this.consumerQueue = consumerQueue;
        this.firstIndex = firstIndex;
    }
    public long getFirstIndex() {
        return firstIndex;
    }

    public Queue<Message> getConsumerQueue() {
        return consumerQueue;
    }

    public void setConsumerQueue(Queue<Message> consumerQueue) {
        this.consumerQueue = consumerQueue;
    }

    @Override
    public void run() {
        AtomicLong nextIndexToConsume = new AtomicLong(firstIndex);

        while(true) {
            if(!consumerQueue.isEmpty()) {
                Message message = consumerQueue.poll();
                if(message.getId() == nextIndexToConsume.get()) {
                    System.out.println("Consumed message with id: " + message.getId());
                    nextIndexToConsume.incrementAndGet();
                } else {
                    throw new RuntimeException("Expected message id: "
                            + nextIndexToConsume.get()
                            + " but found: "
                            + message.getId());
                }
            }
        }
    }
}
