package com.boom.stream.order.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/26 14:44
 */
@Data
public class BmOrder {

    private Long id;

    /**
     * 第三方商户订单号
     **/
    private String outTradeNo;

    /**
     * appId
     **/
    private String appId;

    /**
     * 地区ID
     **/
    private Integer areaId;

    /**
     * 会员id
     **/
    private Long memberId;

    /**
     * 会员昵称
     **/
    private String memberName;

    /**
     * 会员头像
     **/
    private String memberAvatarUrl;

    /**
     * 会员微信openId
     **/
    private String memberWxOpenId;

    /**
     * 会员微信AppId
     **/
    private String memberWxAppId;

    /**
     * 购买总数量
     **/
    private Integer totalBuyNum;

    /**
     * 订单总金额
     **/
    private BigDecimal totalMoney;

    /**
     * 优惠总金额
     **/
    private BigDecimal totalDiscountMoney;

    /**
     * 商户优惠总金额
     **/
    private BigDecimal totalPlatformDiscountMoney;

    /**
     * 平台优惠总金额
     **/
    private BigDecimal totalBusinessDiscountMoney;

    /**
     * 应支付总金额
     **/
    private BigDecimal payMoney;

    /**
     * 实际支付总金额
     **/
    private BigDecimal actualPayMoney;

    /**
     * 订单状态:0=待支付，1=待核销，2=已核销，3=已过期，4=已关闭
     **/
    private Integer orderStatus;

    /**
     * 支付状态:0=待支付，1=用户支付成功，2=用户支付失败，3=回调支付成功，4=回调支付失败
     **/
    private Integer payStatus;

    /**
     * 退款状态，0=未全部退款，1=已全部申请退款，2=已全部退款
     **/
    private Integer refundStatus;

    /**
     * 下单时间
     **/
    private Date orderTime;

    /**
     * 过期时间
     **/
    private Date overdueTime;

    /**
     * 支付时间
     **/
    private Date payTime;

    /**
     * 支付方式0=无1=微信
     **/
    private Integer payType;

    /**
     * 联系方式
     **/
    private String mobile;

    /**
     * 订单备注
     **/
    private String notes;

    /**
     * 购买模式：0=普通，1=霸王餐，2=openapi，3=限时特卖
     **/
    private Integer buyType;

    /**
     * 下单场景：0=微信小程序，1=微信H5，2=openapi，3=抖音小程序
     **/
    private Integer orderScene;

    /**
     * 评价状态：0=未评价，1=已评价
     **/
    private Integer commentStatus;

    /**
     * 会员是否删除订单状态0=否1=是
     **/
    private Integer memberDeleteStatus;

    /**
     * 请求来源
     * 1:微信小程序
     * 2:H5
     * 3:后台
     * 4:抖音小程序
     * 5:openapi
     */
    private Integer requestSource;

    /**
     * 预订补差金额
     */
    private BigDecimal reservationSubsidiesMoney;

    /**
     * 主预订id
     */
    private Long masterReservationId;

    /**
     * 订单类型：0=普通订单，1=礼包订单
     **/
    private Integer orderType;

    private Integer tenantId;

}
