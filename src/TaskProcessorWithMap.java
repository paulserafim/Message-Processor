import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class TaskProcessorWithMap implements Runnable {

    private final AtomicLong nextIdToProcess;
    private final Map<Long,Message> messageMap;
    private Queue<Message> inputMessageQueue;
    private Queue<Message> outputMessageQueue;
    private long threadNumber;
    public TaskProcessorWithMap(Queue<Message> inputMessageQueue, Queue<Message> outputMessageQueue, Map<Long,Message> messageMap, int threadNumber, AtomicLong nextIdToProcess) {
        this.inputMessageQueue = inputMessageQueue;
        this.outputMessageQueue = outputMessageQueue;
        this.messageMap = messageMap;
        this.threadNumber = threadNumber;
        this.nextIdToProcess = nextIdToProcess;
    }

    @Override
    public void run() {
        System.out.println("Start processing thread:" + this.threadNumber);
        while(true) {
            try {
                Message message = getMessageFromQueue(inputMessageQueue);
                if(message != null) {
                    processMessage(message);
                    addMessageToMap(message, messageMap);
                    System.out.println("Message id: " +  message.getId() + " added to a temp map by thread " + threadNumber);
                    enqueueIfNextOrProcessed(message, outputMessageQueue);
                } else {
                    System.out.println("There is no message to process for thread: " + threadNumber);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private synchronized void addMessageToMap(Message message, Map<Long,Message> messageMap) {
        if(message != null) {
            messageMap.put(message.getId(), message);
        }
    }

    private void enqueueIfNextOrProcessed(Message message, Queue<Message> outputMessageQueue) {
        while(true) {
            if(message.getId() == nextIdToProcess.get()) {
                outputMessageQueue.add(message);

                System.out.println("Message with id: "
                        + message.getId()
                        + " with processing time: "
                        + message.getProcessingTime()
                        + " added (from thread:"
                        + threadNumber
                        + ")"
                );

                nextIdToProcess.incrementAndGet();
                break;
            } else {
                synchronized (outputMessageQueue) {
                    if (messageMap.containsKey(nextIdToProcess.get())) {
                        Message messageToPush = messageMap.get(nextIdToProcess.get());
                        outputMessageQueue.add(messageToPush);
                        messageMap.remove(nextIdToProcess.get());
                        System.out.println("Message id: " + nextIdToProcess.get() + " removed from map");
                        outputMessageQueue.notifyAll();
                        nextIdToProcess.incrementAndGet();
                    }
                    break;
                }
            }
        }
    }

    private synchronized Message getMessageFromQueue(Queue<Message> messageQueue) {
        Message message = null;
        if(!messageQueue.isEmpty()) {
            message = inputMessageQueue.poll();
        }
        return message;
    }

    private void processMessage(Message message) throws InterruptedException {
        System.out.println("Thread: "
                + threadNumber
                + " started processing message id: "
                + message.getId()
                + " with processing time of: "
                + message.getProcessingTime()
        );
        Thread.sleep(message.getProcessingTime());
    }
}
