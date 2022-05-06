# Security categories

Manage `security categorie` for a specific project. Security categories can be used to restrict access to a resource. Applying a security category to a resource means that only principals (users or service accounts) that also have this security category can access the resource. To learn more about security categories please read [this page.](https://docs.cognite.com/dev/guides/iam/authorization/)

> Note: To create client see [Client Setup](clientSetup.md)

### List security categories

Retrieves a list of all security categories for a project.

```java
List<SecurityCategory> listSecurityCategoriesResults = new ArrayList<>(); 
client.securityCategories(). 
          list(Request.create()) 
          .forEachRemaining(labels -> listSecurityCategoriesResults.addAll(labels)); 

 
```

### Create security categories

Creates security categories with the given names. Duplicate names in the request are ignored. If a security category with one of the provided names exists already, then the request will fail and no security categories are created.

```java
List<SecurityCategory> createSecurityCategoriesList = 
       List.of(SecurityCategory.newBuilder().setName("Name").build()); 
client.securityCategories().create(createSecurityCategoriesList); 
 
```

### Delete security categories

Deletes the security categories that match the provided IDs. If any of the provided IDs does not belong to an existing security category, then the request will fail and no security categories are deleted.

```java
List<SecurityCategory> deleteItemsResults = 
          client.securityCategories() 
          .delete(listSecurityCategoriesResults); 

```