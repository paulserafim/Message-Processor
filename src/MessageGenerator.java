import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageGenerator implements Runnable {
    private final long firstIndex;
    private final long timeFrequency;
    private final long minTime;
    private final long maxTime;
    private Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();

    public Queue<Message> getMessageQueue() {
        return messageQueue;
    }

    public long getFirstIndex() {
        return firstIndex;
    }

    public long getTimeFrequency() {
        return timeFrequency;
    }

    public long getMinTime() {
        return minTime;
    }

    public long getMaxTime() {
        return maxTime;
    }

    public void setMessageQueue(Queue<Message> messageQueue) {
        this.messageQueue = messageQueue;
    }

    public MessageGenerator(long timeFrequency, final long firstIndex, long minTime, long maxTime) {
        this.timeFrequency = timeFrequency;
        this.firstIndex = firstIndex;
        this.minTime = minTime;
        this.maxTime = maxTime;
    }

    @Override
    public void run() {
        long index = firstIndex;
        System.out.println("Message generating thread has started");
        while(true) {
            long time = (long) (Math.random() * (maxTime - minTime + 1)) + minTime;
            Message message = new Message(index ++, time);
            messageQueue.add(message);
            try {
                Thread.sleep(timeFrequency);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
