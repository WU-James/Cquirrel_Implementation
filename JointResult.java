package org.example;

import java.time.LocalDate;

public class JointResult {
    private int orderKey;
    private LocalDate orderDate;
    private int shipPriority;
    private double revenue;

    // No-argument constructor (required for Flink or serialization)
    public JointResult() {}

    // Constructor
    public JointResult(int orderKey, LocalDate orderDate, int shipPriority, double revenue) {
        this.orderKey = orderKey;
        this.orderDate = orderDate;
        this.shipPriority = shipPriority;
        this.revenue = revenue;
    }

    // Getters and Setters
    public int getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(int orderKey) {
        this.orderKey = orderKey;
    }

    public LocalDate getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(LocalDate orderDate) {
        this.orderDate = orderDate;
    }

    public int getShipPriority() {
        return shipPriority;
    }

    public void setShipPriority(int shipPriority) {
        this.shipPriority = shipPriority;
    }

    public double getRevenue() {
        return revenue;
    }

    public void setRevenue(double revenue) {
        this.revenue = revenue;
    }

    // toString method for easy printing
    @Override
    public String toString() {
        return "Order{" +
                "orderKey=" + orderKey +
                ", orderDate=" + orderDate +
                ", shipPriority=" + shipPriority +
                ", revenue=" + revenue +
                '}';
    }
}