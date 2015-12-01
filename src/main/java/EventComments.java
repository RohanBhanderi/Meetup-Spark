import java.io.Serializable;


public class EventComments implements Serializable{
	
	private static final long serialVersionUID = 6619052268041744360L;
	private String comment;
	private String id;
	private String event_name;
	private String event_id;
	private String member_id;
	private String member_name;
	private String group_city;
	private String group_country;
	private String group_state;
	private String group_id;
	private String group_name;
	private String group_lon;
	private String group_lat;
	private String group_cat;
	private String group_cat_id;
	
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getEvent_name() {
		return event_name;
	}
	public void setEvent_name(String event_name) {
		this.event_name = event_name;
	}
	public String getEvent_id() {
		return event_id;
	}
	public void setEvent_id(String event_id) {
		this.event_id = event_id;
	}
	public String getMember_id() {
		return member_id;
	}
	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}
	public String getMember_name() {
		return member_name;
	}
	public void setMember_name(String member_name) {
		this.member_name = member_name;
	}
	public String getGroup_city() {
		return group_city;
	}
	public void setGroup_city(String group_city) {
		this.group_city = group_city;
	}
	public String getGroup_country() {
		return group_country;
	}
	public void setGroup_country(String group_country) {
		this.group_country = group_country;
	}
	public String getGroup_state() {
		return group_state;
	}
	public void setGroup_state(String group_state) {
		this.group_state = group_state;
	}
	public String getGroup_id() {
		return group_id;
	}
	public void setGroup_id(String group_id) {
		this.group_id = group_id;
	}
	public String getGroup_name() {
		return group_name;
	}
	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}
	public String getGroup_lon() {
		return group_lon;
	}
	public void setGroup_lon(String group_lon) {
		this.group_lon = group_lon;
	}
	public String getGroup_lat() {
		return group_lat;
	}
	public void setGroup_lat(String group_lat) {
		this.group_lat = group_lat;
	}
	public String getGroup_cat() {
		return group_cat;
	}
	public void setGroup_cat(String group_cat) {
		this.group_cat = group_cat;
	}
	public String getGroup_cat_id() {
		return group_cat_id;
	}
	public void setGroup_cat_id(String group_cat_id) {
		this.group_cat_id = group_cat_id;
	}
}