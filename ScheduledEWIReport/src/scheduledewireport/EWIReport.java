/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scheduledewireport;

import java.util.Date;

/**
 *
 * @author Refly IDFA
 */
public class EWIReport {

    private int orderNumber;
    private int salesOrderItem;
    private int sapItemID;
    private int scSalesOrderItem;
    private String paymentMethod;
    private String itemStatus;
    private Date orderDate;
    private Date verifiedDate;
    private Date shippedDate;
    private Date deliveredDate;
    private Date deliveredDateInput;
    private Date returnedDate;
    private Date replacedDate;
    private Date refundedDate;
    private double unitPrice;
    private double paidPrice;
    private double couponMoneyValue;
    private double cartRuleDiscount;
    private double shippingAmount;
    private double shippingSurcharge;
    private double itemPriceCredit;
    private double commission;
    private double paymentFee;
    private double shippingFeeCredit;
    private double itemPrice;
    private double commissionCredit;
    private double shippingFee;
    private double sellerCredit;
    private double sellerCreditItem;
    private double otherFee;
    private double sellerDebitItem;
    private double amountPaidSeller;
    private String transactionID;
    private Date transactionDate;
    private Date statementTimeFrameStart;
    private Date statementTimeFrameEnd;
    private String sku;
    private int districtID;
    private String awb;
    private String shipmentProvider;
    private String scIDSeller;
    private String sellerName;
    private String legalName;
    private String taxClass;

    public int getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(int orderNumber) {
        this.orderNumber = orderNumber;
    }

    public int getSalesOrderItem() {
        return salesOrderItem;
    }

    public void setSalesOrderItem(int salesOrderItem) {
        this.salesOrderItem = salesOrderItem;
    }

    public int getSapItemID() {
        return sapItemID;
    }

    public void setSapItemID(int sapItemID) {
        this.sapItemID = sapItemID;
    }

    public int getScSalesOrderItem() {
        return scSalesOrderItem;
    }

    public void setScSalesOrderItem(int scSalesOrderItem) {
        this.scSalesOrderItem = scSalesOrderItem;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getItemStatus() {
        return itemStatus;
    }

    public void setItemStatus(String itemStatus) {
        this.itemStatus = itemStatus;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public Date getVerifiedDate() {
        return verifiedDate;
    }

    public void setVerifiedDate(Date verifiedDate) {
        this.verifiedDate = verifiedDate;
    }

    public Date getShippedDate() {
        return shippedDate;
    }

    public void setShippedDate(Date shippedDate) {
        this.shippedDate = shippedDate;
    }

    public Date getDeliveredDate() {
        return deliveredDate;
    }

    public void setDeliveredDate(Date deliveredDate) {
        this.deliveredDate = deliveredDate;
    }

    public Date getDeliveredDateInput() {
        return deliveredDateInput;
    }

    public void setDeliveredDateInput(Date deliveredDateInput) {
        this.deliveredDateInput = deliveredDateInput;
    }

    public Date getReturnedDate() {
        return returnedDate;
    }

    public void setReturnedDate(Date returnedDate) {
        this.returnedDate = returnedDate;
    }

    public Date getReplacedDate() {
        return replacedDate;
    }

    public void setReplacedDate(Date replacedDate) {
        this.replacedDate = replacedDate;
    }

    public Date getRefundedDate() {
        return refundedDate;
    }

    public void setRefundedDate(Date refundedDate) {
        this.refundedDate = refundedDate;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public double getPaidPrice() {
        return paidPrice;
    }

    public void setPaidPrice(double paidPrice) {
        this.paidPrice = paidPrice;
    }

    public double getCouponMoneyValue() {
        return couponMoneyValue;
    }

    public void setCouponMoneyValue(double couponMoneyValue) {
        this.couponMoneyValue = couponMoneyValue;
    }

    public double getCartRuleDiscount() {
        return cartRuleDiscount;
    }

    public void setCartRuleDiscount(double cartRuleDiscount) {
        this.cartRuleDiscount = cartRuleDiscount;
    }

    public double getShippingAmount() {
        return shippingAmount;
    }

    public void setShippingAmount(double shippingAmount) {
        this.shippingAmount = shippingAmount;
    }

    public double getShippingSurcharge() {
        return shippingSurcharge;
    }

    public void setShippingSurcharge(double shippingSurcharge) {
        this.shippingSurcharge = shippingSurcharge;
    }

    public double getItemPriceCredit() {
        return itemPriceCredit;
    }

    public void setItemPriceCredit(double itemPriceCredit) {
        this.itemPriceCredit = itemPriceCredit;
    }

    public double getCommission() {
        return commission;
    }

    public void setCommission(double commission) {
        this.commission = commission;
    }

    public double getPaymentFee() {
        return paymentFee;
    }

    public void setPaymentFee(double paymentFee) {
        this.paymentFee = paymentFee;
    }

    public double getShippingFeeCredit() {
        return shippingFeeCredit;
    }

    public void setShippingFeeCredit(double shippingFeeCredit) {
        this.shippingFeeCredit = shippingFeeCredit;
    }

    public double getItemPrice() {
        return itemPrice;
    }

    public void setItemPrice(double itemPrice) {
        this.itemPrice = itemPrice;
    }

    public double getCommissionCredit() {
        return commissionCredit;
    }

    public void setCommissionCredit(double commissionCredit) {
        this.commissionCredit = commissionCredit;
    }

    public double getShippingFee() {
        return shippingFee;
    }

    public void setShippingFee(double shippingFee) {
        this.shippingFee = shippingFee;
    }

    public double getSellerCredit() {
        return sellerCredit;
    }

    public void setSellerCredit(double sellerCredit) {
        this.sellerCredit = sellerCredit;
    }

    public double getSellerCreditItem() {
        return sellerCreditItem;
    }

    public void setSellerCreditItem(double sellerCreditItem) {
        this.sellerCreditItem = sellerCreditItem;
    }

    public double getOtherFee() {
        return otherFee;
    }

    public void setOtherFee(double otherFee) {
        this.otherFee = otherFee;
    }

    public double getSellerDebitItem() {
        return sellerDebitItem;
    }

    public void setSellerDebitItem(double sellerDebitItem) {
        this.sellerDebitItem = sellerDebitItem;
    }

    public double getAmountPaidSeller() {
        return amountPaidSeller;
    }

    public void setAmountPaidSeller(double amountPaidSeller) {
        this.amountPaidSeller = amountPaidSeller;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }

    public void setTransactionDate(Date transactionDate) {
        this.transactionDate = transactionDate;
    }

    public Date getStatementTimeFrameStart() {
        return statementTimeFrameStart;
    }

    public void setStatementTimeFrameStart(Date statementTimeFrameStart) {
        this.statementTimeFrameStart = statementTimeFrameStart;
    }

    public Date getStatementTimeFrameEnd() {
        return statementTimeFrameEnd;
    }

    public void setStatementTimeFrameEnd(Date statementTimeFrameEnd) {
        this.statementTimeFrameEnd = statementTimeFrameEnd;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public int getDistrictID() {
        return districtID;
    }

    public void setDistrictID(int districtID) {
        this.districtID = districtID;
    }

    public String getAwb() {
        return awb;
    }

    public void setAwb(String awb) {
        this.awb = awb;
    }

    public String getShipmentProvider() {
        return shipmentProvider;
    }

    public void setShipmentProvider(String shipmentProvider) {
        this.shipmentProvider = shipmentProvider;
    }

    public String getScIDSeller() {
        return scIDSeller;
    }

    public void setScIDSeller(String scIDSeller) {
        this.scIDSeller = scIDSeller;
    }

    public String getSellerName() {
        return sellerName;
    }

    public void setSellerName(String sellerName) {
        this.sellerName = sellerName;
    }

    public String getLegalName() {
        return legalName;
    }

    public void setLegalName(String legalName) {
        this.legalName = legalName;
    }

    public String getTaxClass() {
        return taxClass;
    }

    public void setTaxClass(String taxClass) {
        this.taxClass = taxClass;
    }
}
