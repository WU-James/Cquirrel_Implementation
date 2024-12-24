package org.example;

import java.time.LocalDate;

public class Joint_LineItem_Order {
    private boolean insertion;
    private int orderKey;
    private LocalDate orderDate;
    private int shipPriority;
    private double extendedPrice;
    private double discount;

    // No-argument constructor (required for Flink or serialization)
    public Joint_LineItem_Order() {}

    // Constructor
    public Joint_LineItem_Order(boolean insertion, int orderKey, LocalDate orderDate, int shipPriority, double extendedPrice, double discount) {
        this.insertion = insertion; // Initialize the new member variable
        this.orderKey = orderKey;
        this.orderDate = orderDate;
        this.shipPriority = shipPriority;
        this.extendedPrice = extendedPrice;
        this.discount = discount;
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

    public double getExtendedPrice() {
        return extendedPrice;
    }

    public void setExtendedPrice(double extendedPrice) {
        this.extendedPrice = extendedPrice;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }

    // toString method for easy printing
    @Override
    public String toString() {
        return "Joint_LineItem_Order{" +
                "insertion=" + insertion + // Include insertion in the toString
                ", orderKey=" + orderKey +
                ", orderDate=" + orderDate +
                ", shipPriority=" + shipPriority +
                ", extendedPrice=" + extendedPrice +
                ", discount=" + discount +
                '}';
    }
}