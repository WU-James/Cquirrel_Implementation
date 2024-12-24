package org.example;

import java.time.LocalDate;

public class LineItem {
    private boolean insertion;
    private int orderKey;
    private double extendedPrice;
    private double discount;
    private LocalDate shipDate;
    private int lineNumber; // New member variable

    // No-argument constructor (required for Flink or serialization)
    public LineItem() {}

    // Parameterized constructor
    public LineItem(boolean insertion, int orderKey, double extendedPrice, double discount, LocalDate shipDate, int lineNumber) {
        this.insertion = insertion;
        this.orderKey = orderKey;
        this.extendedPrice = extendedPrice;
        this.discount = discount;
        this.shipDate = shipDate;
        this.lineNumber = lineNumber; // Initialize the new member variable
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

    public LocalDate getShipDate() {
        return shipDate;
    }

    public void setShipDate(LocalDate shipDate) {
        this.shipDate = shipDate;
    }

    public int getLineNumber() { // Getter for lineNumber
        return lineNumber;
    }

    public void setLineNumber(int lineNumber) { // Setter for lineNumber
        this.lineNumber = lineNumber;
    }

    @Override
    public String toString() {
        return "LineItem{" +
                "insertion=" + insertion +
                ", orderKey=" + orderKey +
                ", extendedPrice=" + extendedPrice +
                ", discount=" + discount +
                ", shipDate=" + shipDate +
                ", lineNumber=" + lineNumber + // Include lineNumber in the toString
                '}';
    }
}