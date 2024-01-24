public class Message {
    private long id;
    private long processingTime;
    public Message(long id, long processingTime) {
        this.id = id;
        this.processingTime = processingTime;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public void setProcessingTime(long processingTime) {
        this.processingTime = processingTime;
    }
}
