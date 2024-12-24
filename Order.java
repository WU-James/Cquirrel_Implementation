package org.example;

import java.time.LocalDate;

public class Order {
    private boolean insertion;
    private int orderKey;
    private int custKey;
    private LocalDate orderDate;
    private int shipPriority;

    // No-argument constructor (required for frameworks and serialization)
    public Order() {}

    // Parameterized constructor
    public Order(boolean insertion, int orderKey, int custKey, LocalDate orderDate, int shipPriority) {
        this.insertion = insertion; // Initialize the new member variable
        this.orderKey = orderKey;
        this.custKey = custKey;
        this.orderDate = orderDate;
        this.shipPriority = shipPriority;
    }

    // Getters and Setters
    public boolean isInsertion() { // Getter for insertion
        return insertion;
    }

    public void setInsertion(boolean insertion) { // Setter for insertion
        this.insertion = insertion;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(int orderKey) {
        this.orderKey = orderKey;
    }

    public int getCustKey() {
        return custKey;
    }

    public void setCustKey(int custKey) {
        this.custKey = custKey;
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

    @Override
    public String toString() {
        return "Order{" +
                "insertion=" + insertion + // Include insertion in the toString
                ", orderKey=" + orderKey +
                ", custKey=" + custKey +
                ", orderDate=" + orderDate +
                ", shipPriority=" + shipPriority +
                '}';
    }
}