import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MessageProcessorWithoutMap implements MessageProcessor{
    private final long firstIndex;
    private final short numberOfThreads;
    private Queue<Message> inputMessageQueue;

    private Queue<Message> outputMessageQueue = new ConcurrentLinkedQueue<>();

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

    public MessageProcessorWithoutMap(Queue<Message> inputMessageQueue, long firstIndex, short numberOfThreads) {
        this.inputMessageQueue = inputMessageQueue;
        this.firstIndex = firstIndex;
        this.numberOfThreads = numberOfThreads;
    }

    public void process() {
        AtomicLong nextIdToProcess = new AtomicLong(firstIndex);

        for(int index = 0; index < numberOfThreads; index++) {
            TaskProcessorWithoutMap taskProcessorWithoutMap = new TaskProcessorWithoutMap(inputMessageQueue, outputMessageQueue, index, nextIdToProcess);
            Thread processThread = new Thread(taskProcessorWithoutMap);
            processThread.start();
        }
    }
}
