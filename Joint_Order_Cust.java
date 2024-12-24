package org.example;

import java.time.LocalDate;

public class Joint_Order_Cust {
    private boolean insertion;
    private int orderKey;
    private LocalDate orderDate;
    private int shipPriority;

    // No-argument constructor (required for frameworks and serialization)
    public Joint_Order_Cust() {}

    // Parameterized constructor
    public Joint_Order_Cust(boolean insertion, int orderKey, LocalDate orderDate, int shipPriority) {
        this.insertion = insertion;
        this.orderKey = orderKey;
        this.orderDate = orderDate;
        this.shipPriority = shipPriority;
    }

    // Getters and Setters
    public boolean isInsertion() {
        return insertion;
    }

    public void setInsertion(boolean insertion) {
        this.insertion = insertion;
    }

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

    @Override
    public String toString() {
        return "Order{" +
                "insertion=" + insertion +
                ", orderKey=" + orderKey +
                ", orderDate=" + orderDate +
                ", shipPriority=" + shipPriority +
                '}';
    }
}
