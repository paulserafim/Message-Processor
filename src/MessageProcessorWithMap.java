import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MessageProcessorWithMap implements MessageProcessor{
    private final long firstIndex;
    private final short numberOfThreads;
    private Map<Long, Message> messageMap = new ConcurrentHashMap<>();
    private Queue<Message> inputMessageQueue;
    private Queue<Message> outputMessageQueue = new ConcurrentLinkedQueue<>();
    public long getFirstIndex() {
        return firstIndex;
    }

    public Map<Long, Message> getMessageMap() {
        return messageMap;
    }

    public void setMessageMap(Map<Long, Message> messageMap) {
        this.messageMap = messageMap;
    }

    public Queue<Message> getInputMessageQueue() {
        return inputMessageQueue;
    }

    public void setInputMessageQueue(Queue<Message> inputMessageQueue) {
        this.inputMessageQueue = inputMessageQueue;
    }

    public Queue<Message> getOutputMessageQueue() {
        return outputMessageQueue;
    }

    public void setOutputMessageQueue(Queue<Message> outputMessageQueue) {
        this.outputMessageQueue = outputMessageQueue;
    }

    public MessageProcessorWithMap(Queue<Message> inputMessageQueue, long firstIndex, short numberOfThreads) {
        this.inputMessageQueue = inputMessageQueue;
        this.firstIndex = firstIndex;
        this.numberOfThreads = numberOfThreads;
    }

    @Override
    public void process() {
        AtomicLong nextIdToProcess = new AtomicLong(firstIndex);

        for(int index = 0; index < numberOfThreads; index++) {
            TaskProcessorWithMap taskProcessorWithMap = new TaskProcessorWithMap(inputMessageQueue, outputMessageQueue, messageMap, index, nextIdToProcess);
            Thread processThread = new Thread(taskProcessorWithMap);
            processThread.start();
        }
    }
}
