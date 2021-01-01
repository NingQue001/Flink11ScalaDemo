package com.anven.windowDemo;

public class Order {
    long user;
    String product;
    int amount;
    long ts;

    Order(long user, String product, int amount, long ts) {
        this.user = user;
        this.product = product;
        this.amount = amount;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                ", ts=" + ts +
                '}';
    }

    public long getUser() {
        return user;
    }

    public void setUser(long user) {
        this.user = user;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
