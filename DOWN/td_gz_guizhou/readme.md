### 脚本说明

脚本启动: 本脚本基于scrapy框架开发,  启动时进入到相对路径的\IntegrationSpider\IntegrationSpider\spiders目录下, 运行 **scrapy runspider project 命令**即可启动项目



supplyResult.py即供应结果字段说明, 字段名称依次是:   administration, parcelLocation, totalArea, landPurpose, signTime, projectName, projectLocation, area, landSource, supplyType, landUsegeTerm, classification, landLevel, transferPrice, issue, paymentDate, paymentAmount, remark, landHolder, plotRatioUP, plotRatioLOWER, agreedDeliveryTime, agreedStartTime, agreedCompletionTime, actualStartTime, actualCompletionTime, approvedUnit, contractTime, crawlingTime, url, md5Mark



dealFormula.py即成交公示段说明, 字段名称依次是:  administration, supplyNoticeTitle, publishTime, fileTitle, textTitle, projectName,parcelNumber, parcelLocation, landPurpose, landArea, transferTimeLimit, transferPrice, landPurposeDetail,transferUnit, remark, publicityPeriod, contactUnit, unitAddr, postalCode, contactTel, contacter, email,lanServiceCondition, crawlingTime, url, md5Mark



supplyLandPlan.py即供地计划字段说明, 字段名称依次是:  administration,supplyLandTitle,publishTime,fileTitle,totalSupplyLand,yearSupplyPlan,industrialLand,businessLand,totalHousionSupply,low_rentLand,affordableHousing,pengGaiLand,low_rentpengGaiLand,pengGaiAffordableHousing,pengGaiCommercialHousing,commercialHousing,ortherHousingLand,publicServiceLand,transportationLand,waterAreaLand,specialLand,publicRentalLand,limitCommercialLand,mediumCommercialLand,totalCommercialLand,commercialRatio, crawlingTime, url, md5Mark



transferNotice.py即出让公告字段说明, 字段名称依次是: fileTitle, textTitle, noticeType, administration, supplyNoticeTitle, publishTime, transferTime,transferAddr,depositTime, affirmBuyTime, address, tel, linkman, accountOpener, depositBank, bankAccount, parcelNumber,parcelArea, parcelLocation, transferTimeLimit, plotRatio, buildingDensity, greenRatio, buldingHP,landPurpose, investmentIntensity, cashDeposit, evaluateNum, landCondition, startPrice, bidIncrenment,hangOutDeadTime, hangOutStartTime, supportingInfrastructure,landItact, sewageDisposalFacility, remark, crawlingTime, url, md5Mark

