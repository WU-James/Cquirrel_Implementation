package org.example;

public class Customer {
    private boolean insertion;
    private int custKey;
    private String mktSegment;

    // No-argument constructor (required for frameworks and serialization)
    public Customer() {}

    // Parameterized constructor
    public Customer(boolean insertion, int custKey, String mktSegment) {
        this.insertion = insertion; // Initialize the new member variable
        this.custKey = custKey;
        this.mktSegment = mktSegment;
    }

    // Getters and Setters
    public boolean isInsertion() { // Getter for insertion
        return insertion;
    }

    public void setInsertion(boolean insertion) { // Setter for insertion
        this.insertion = insertion;
    }

    public int getCustKey() {
        return custKey;
    }

    public void setCustKey(int custKey) {
        this.custKey = custKey;
    }

    public String getMktSegment() {
        return mktSegment;
    }

    public void setMktSegment(String mktSegment) {
        this.mktSegment = mktSegment;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "insertion=" + insertion + // Include insertion in the toString
                ", custKey=" + custKey +
                ", mktSegment='" + mktSegment + '\'' +
                '}';
    }
}