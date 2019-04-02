package com.kafka.kafkaSparkNoSQL;

public class Item {
	
	private String item;
	private String description;
	private int count;
	private double percentage;
	
	public Item() {}
	public Item(String item, String description, int count, double percentage) {
		super();
		this.item = item;
		this.description = description;
		this.count = count;
		this.percentage = percentage;
	}
	
	public String getItem() {
		return item;
	}
	public void setItem(String item) {
		this.item = item;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public double getPercentage() {
		return percentage;
	}
	public void setPercentage(double percentage) {
		this.percentage = percentage;
	}
}
