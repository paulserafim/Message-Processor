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
        try {
            consumeWhenNotified(consumerQueue);
            //consumeByPolling(consumerQueue, 1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void consumeWhenNotified(Queue<Message> consumerQueue) throws InterruptedException {
        AtomicLong nextIndexToConsume = new AtomicLong(firstIndex);

        while(true) {
            synchronized (consumerQueue) {
                Message message = getMessageFromQueue(consumerQueue);
                if(message != null) {
                    if(messageIsNext(message, nextIndexToConsume)) {
                        System.out.println("Consumed message with id: " + message.getId());
                        nextIndexToConsume.incrementAndGet();
                    } else {
                        throw new RuntimeException("Expected message id: "
                                + nextIndexToConsume.get()
                                + " but found: "
                                + message.getId());
                    }
                } else {
                    consumerQueue.wait();
                }
            }
        }
    }

    private void consumeByPolling(Queue<Message> messageQueue, long pollingRate) throws InterruptedException {
        AtomicLong nextIndexToConsume = new AtomicLong(firstIndex);

        while(true) {
            Thread.sleep(pollingRate);
            if(!consumerQueue.isEmpty()) {
                Message message = getMessageFromQueue(consumerQueue);
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

    private synchronized Message getMessageFromQueue(Queue<Message> messageQueue) {
        Message message = null;
        if(!messageQueue.isEmpty()) {
            message = messageQueue.poll();
        }
        return message;
    }

    private boolean messageIsNext(Message message, AtomicLong nextIndexToConsume) {
        return message.getId() == nextIndexToConsume.get();
    }
}
