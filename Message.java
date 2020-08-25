import com.google.gson.Gson;

public class Message implements Comparable<Message> {
	public enum Type {
		REQUEST,
		REPLY,
		RELEASE,
		FK,
		FKK,

		UPDATE,
		READ,
		RESPONSE
	}

	public enum Result {
		SUCCESS,
		FAIL
	}

	Type type;
	Result res;
	int key;
	Object value;
	int timestamp;
	int from;
	int to;
	private static Gson gson;

	public Message(Type type, Result res, Integer key, Object value, int timestamp, int from, int to) {
		this.type = type;
		this.res = res;
		this.timestamp = timestamp;
		this.from = from;
		this.to = to;
		this.key = key;
		this.value = value;
	}

	@Override
    public int compareTo(Message that) {
        return this.timestamp == that.timestamp ? this.from - that.from : this.timestamp - that.timestamp;
    }

    @Override
    public String toString() {
        if (gson == null)
            gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public Message clone() {
        return new Message(this.type, this.res, this.key, this.value, this.timestamp, this.from, this.to);
    }
}